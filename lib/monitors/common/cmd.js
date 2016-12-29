/**
 * Created by frank on 16-12-26.
 */
const logger = require('pomelo-logger').getLogger('pomelo', __filename),
	vm = require('vm'),
	_ = require('lodash'),
	util = require('util'),
	Constants = require('../../util/constants');

class Common
{
	static Init(client, data)
	{
		logger.debug(`server: ${client.app.serverId} receive command, with data: ${data}`);
		if (!data)
		{
			logger.warn('server: %s command data is null.', client.app.serverId);
			return;
		}
		if (_.isString(data))
		{
			data = JSON.parse(data);
		}

		switch (data.command)
		{
		case 'stop':
			CommonUtility.Stop(client);
			break;
		case 'kill':
			CommonUtility.Kill(client);
			break;
		case 'addCron':
			CommonUtility.AddCron(client, data);
			break;
		case 'removeCron':
			CommonUtility.RemoveCron(client, data);
			break;
		case 'blacklist':
			CommonUtility.AddBlacklist(client, data);
			break;
		case 'set':
			CommonUtility.Set(client, data);
			break;
		case 'get':
			CommonUtility.Get(client, data);
			break;
		case 'enable':
			CommonUtility.Enable(client, data);
			break;
		case 'disable':
			CommonUtility.Disable(client, data);
			break;
		case 'run':
			CommonUtility.Run(client, data);
			break;
		case 'exec':
			CommonUtility.Exec(client, data);
			break;
		case 'show':
			CommonUtility.Show(client);
			break;
		default:
			logger.debug('server: %s receive unknown command, with data: %j', client.app.serverId, data);
			break;
		}
	}
}

class CommonUtility
{
	static Stop(client)
	{
		logger.info(`server : ${client.app.serverId} is stopped`);
		client.app.set(Constants.RESERVED.STOP_FLAG, true);
		client.app.stop();
	}

	static Kill(client)
	{
		logger.info(`server : ${client.app.serverId} is forced killed`);
		process.exit(0);
	}

	static AddCron(client, msg)
	{
		logger.info(`addCron ${msg.cron} to server ${client.app.serverId}`);
		client.app.addCrons([msg.cron]);
	}

	static RemoveCron(client, msg)
	{
		logger.info(`removeCron ${msg.cron} to server ${client.app.serverId}`);
		client.app.removeCrons([msg.cron]);
	}

	static AddBlacklist(client, msg)
	{
		if (client.app.isFrontend())
		{
			logger.info(`addBlacklist ${msg.blacklist} to server ${client.app.serverId}`);
			const connector = client.app.components.__connector__;
			connector.blacklist = connector.blacklist.concat(msg.blacklist);
		}
	}

	static Set(client, msg)
	{
		const key = msg.param['key'];
		const value = msg.param['value'];
		logger.info(`set ${key} to value ${value} in server ${client.app.serverId}`);
		client.app.set(key, value);
	}

	static Get(client, msg)
	{
		let value = client.app.get(msg.param);
		if (!CommonUtility.CheckJson(value))
		{
			value = 'object';
		}
	}

	static Enable(client, msg)
	{
		logger.info(`enable ${msg.param} in server ${client.app.serverId}`);
		client.app.enable(msg.param);
	}

	static Disable(client, msg)
	{
		logger.info(`disable ${msg.param} in server ${client.app.serverId}`);
		client.app.disable(msg.param);
	}

	static Run(client, msg)
	{
		const ctx = {
			app    : client.app,
			result : null
		};
		try
		{
			vm.runInNewContext(`result = ${msg.param}`, ctx, 'myApp.vm');
			logger.info(`run ${msg.param} in server ${client.app.serverId} with result ${util.inspect(ctx.result)}`);
			client.sendCommandResult(util.inspect(ctx.result));
		}
		catch (e)
		{
			logger.error(`run ${msg.param} in server ${client.app.serverId} with err ${e.toString()}`);
			client.sendCommandResult(e.toString());
		}
	}

	static Exec(client, msg)
	{
		const context = {
			app     : client.app,
			require : require,
			os      : require('os'),
			fs      : require('fs'),
			process : process,
			util    : util
		};
		try
		{
			vm.runInNewContext(msg.script, context);
			logger.info(`exec ${msg.script} in server ${client.app.serverId} with result ${context.result}`);
			const result = context.result;
			if (!result)
			{
				client.sendCommandResult('script result should be assigned to result value to script module context');
			}
			else
			{
				client.sendCommandResult(result.toString());
			}
		}
		catch (e)
		{
			logger.error('exec %s in server %s with err %s', msg.script, client.app.serverId, e.toString());
			client.sendCommandResult(e.toString());
		}
	}

	static Show(client)
	{
		const result = {};
		result.connectionInfo = CommonUtility.GetConnectionInfo(client);
		result.proxyInfo = CommonUtility.GetProxyInfo(client);
		result.handlerInfo = CommonUtility.GetHandlerInfo(client);
		result.componentInfo = CommonUtility.GetComponentInfo(client);
		result.settingInfo = CommonUtility.GetSettingInfo(client);

		client.sendCommandResult(JSON.stringify(result), 'show');
	}

	static GetConnectionInfo(client)
	{
		const connectionInfo = {};
		const connection = _.get(client.app.components, '__connection__', null);
		connectionInfo.serverId = client.app.serverId;

		if (connection)
		{
			connectionInfo.connectionInfo = connection.getStatisticsInfo();
		}
		else
		{
			connectionInfo.connectionInfo = 'no connection';
		}
		return connectionInfo;
	}

	static GetProxyInfo(client)
	{
		let proxyInfo = {};
		const proxy = _.get(client.app.components, '__proxy__');
		if (proxy && proxy.client && proxy.client.proxies.user)
		{
			const proxies = proxy.client.proxies.user;
			const server = client.app.getServerById(client.app.serverId);
			if (!server)
			{
				logger.error(`no server with this id ${client.app.serverId}`);
			}
			else
			{
				const type = server['serverType'];
				const tmp = proxies[type];
				proxyInfo[type] = {};
				_.forEach(tmp, (value, _proxy) =>
				{
					proxyInfo[type][_proxy] = {};
					_.forEach(value, (rpcValue, rpcKey) =>
					{
						if (_.isFunction(rpcValue))
						{
							proxyInfo[type][_proxy][rpcKey] = 'function';
						}
					});
				});
			}
		}
		else
		{
			proxyInfo = 'no proxy loaded';
		}
		return proxyInfo;
	}

	static GetHandlerInfo(client)
	{
		let handlerInfo = {};
		const server = _.get(client.app.components, '__server__', null);
		if (server && server.server && server.server.handlerService.handlerMap)
		{
			const handles = server.server.handlerService.handlerMap;
			const serverInfo = client.app.getServerById(client.app.serverId);
			if (!serverInfo)
			{
				logger.error(`no server with this id ${client.app.serverId}`);
			}
			else
			{
				const type = serverInfo['serverType'];
				const tmp = handles;
				handlerInfo[type] = {};
				_.forEach(tmp, (value, key) =>
				{
					handlerInfo[type][key] = {};
					_.forEach(value, (handleValue, handleKey) =>
					{
						_.forEach(handleValue, (sValue, sKey) =>
						{
							if (_.isFunction(sValue))
							{
								handlerInfo[type][key][sKey] = 'function';
							}
						});
					});
				});
			}
		}
		else
		{
			handlerInfo = 'no handler loaded';
		}
		return handlerInfo;
	}

	static GetComponentInfo(client)
	{
		const components = client.app.components;
		const res = {};
		_.forEach(components, (component, key) =>
		{
			key = CommonUtility.GetComponentName(key);
			res[key] = CommonUtility.Clone(key, client.app.get(`${key}Config`));
		});
		return res;
	}

	static GetSettingInfo(client)
	{
		const settings = client.app.settings;
		const res = {};
		_.forEach(settings, (value, key) =>
		{
			if (key.match(/^__\w+__$/) || key.match(/\w+Config$/))
			{
				return;
			}
			if (!CommonUtility.CheckJson(value))
			{
				res[key] = 'Object';
				return;
			}
			res[key] = value;
		});
		return res;
	}

	static GetComponentName(key)
	{
		let t = key.match(/^__(\w+)__$/);
		if (t)
		{
			t = t[1];
		}
		return t;
	}

	static Clone(param, obj)
	{
		const result = {};
		_.forEach(obj, (value, key) =>
		{
			if (_.isFunction(value) || _.isObject(value))
			{
				return;
			}
			result[key] = obj[key];
		});
		return result;
	}

	static CheckJson(obj)
	{
		if (!obj)
		{
			return true;
		}
		try
		{
			JSON.stringify(obj);
		}
		catch (e)
		{
			return false;
		}
		return true;
	}
}

module.exports.init = Common.Init;