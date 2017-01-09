/**
 * Created by frank on 16-12-26.
 */
const _ = require('lodash'),
	zookeeper = require('node-zookeeper-client'),
	crypto = require('crypto'),
	commander = require('./common/cmd'),
	constants = require('../util/constants'),
	utils = require('../util/utils'),
	CreateCountDownLatch = require('../util/CountDownLatch'),
	logger = require('pomelo-logger').getLogger('pomelo', __filename),
	async = require('async');

const ZK_INIT = 0;
const ZK_CONNECTING = 1;
const ZK_CONNECTED = 2;
const ZK_CONNECT_FAIL = 3;
const ZK_RECONNECTING = 4;
const ZK_END = 5;

class ZookeeperMonitor
{
	constructor(app, opts)
	{
		this.state = ZK_INIT;
		this.app = app;
		this.servers = opts.servers;
		this.path = opts.path || constants.RESERVED.DEFAULT_ROOT;
		this.username = opts.username || '';
		this.password = opts.password || '';
		this.timeout = opts.timeout || constants.TIME.DEFAULT_ZK_TIMEOUT;
		this.setACL = opts.setACL;
		this.retries = opts.retries || constants.RETRY.CONNECT_RETRY;
		this.spinDelay = opts.spinDelay || constants.TIME.DEFAULT_SPIN_DELAY;
		this.reconnectTimes = opts.reconnectTimes || constants.RETRY.RECONNECT_RETRY;
		this.nodePath = `${this.path}/${app.serverType}${constants.RESERVED.ZK_NODE_SEP}${app.serverId}`;
		this.cmdPath = `${this.path}/${constants.RESERVED.ZK_NODE_COMMAND}${app.serverId}`;
		this.authentication = `${this.username}:${this.password}`;

		const shaDigest = crypto.createHash('sha1')
			.update(this.authentication)
			.digest('base64');
		this.acls = [
			new zookeeper.ACL(
				zookeeper.Permission.ALL,
				new zookeeper.Id('digest', `${this.username}:${shaDigest}`)
			)
		];

		this.client = zookeeper.createClient(this.servers, {
			sessionTimeout : this.timeout,
			retries        : this.retries,
			spinDelay      : this.spinDelay});
	}

	start(callBack)
	{
		logger.info('try to start monitor server');

		this.cbTimer = setTimeout(() =>
		{
			logger.info('connect to zookeeper timeout');
			this.state = ZK_CONNECT_FAIL;
			utils.invokeCallback(callBack, new Error(`${this.app.serverId} cannot connect to zookeeper.`));
		}, constants.TIME.DEFAULT_ZK_CONNECT_TIMEOUT);

		this.client.once('connected', () =>
		{
			logger.info('%s connect zookeeper successfully.', this.app.serverId);
			this.state = ZK_CONNECTED;
			this.client.addAuthInfo('digest', new Buffer(this.authentication));
			if (this.setACL)
			{
				this.client.setACL(this.path, this.acls, -1, (err, stat) =>
				{
					if (err)
					{
						logger.error('failed to set ACL: %j', err.stack);
						clearTimeout(this.cbTimer);
						throw err;
					}
					clearTimeout(this.cbTimer);
					ZookeeperMonitorUtility.RegisterZK(this, () =>
					{
						ZookeeperMonitorUtility.GetAndWatchCluster(this);
						utils.invokeCallback(callBack);
					});
					logger.info('ACL is set to: %j', this.acls);
				});
			}
			else
			{
				clearTimeout(this.cbTimer);
				ZookeeperMonitorUtility.RegisterZK(this, () =>
				{
					ZookeeperMonitorUtility.GetAndWatchCluster(this);
					utils.invokeCallback(callBack);
				});
			}
		});

		ZookeeperMonitorUtility.DisconnectHandle(this);

		this.state = ZK_CONNECTING;
		this.client.connect();
	}

	stop()
	{
		this.client.close();
	}

	sendCommandResult(result, type)
	{
		const buffer = new Buffer(result);
		this.client.setData(this.cmdPath, buffer, (err, stat) =>
		{
			if (err)
			{
				logger.error('send result to zookeeper failed with err:%j', err);
			}
		});
	}
}

class ZookeeperMonitorUtility
{
	static GetData(zk, path, watcher, cb)
	{
		zk.client.getData(path, watcher, (err, data) =>
		{
			if (err)
			{
				utils.invokeCallback(cb, err);
				return;
			}
			utils.invokeCallback(cb, null, _.isNil(data) ? null : data.toString());
		});
	}

	static GetAndWatchCluster(zk)
	{
		logger.debug('watch server: %j, with path: %s', zk.app.serverId, zk.nodePath);
		ZookeeperMonitorUtility.GetChildren(zk, () =>
		{
			ZookeeperMonitorUtility.GetAndWatchCluster(zk);
		},
		(err, children) =>
		{
			if (err)
			{
				logger.error('get children failed when watch server, with err: %j', err.stack);
				return;
			}
			logger.debug('cluster children: %j', children);
			ZookeeperMonitorUtility.GetClusterInfo(zk.app, zk, children);
		});
	}

	static RegisterZK(zk, cb)
	{
		const allInfo = {};
		const serverInfo = zk.app.getCurServer();
		serverInfo.pid = process.pid;
		allInfo.serverInfo = serverInfo;

		const buffer = new Buffer(JSON.stringify(allInfo));
		async.series(
			[
				callback =>
				{
					ZookeeperMonitorUtility.CreateDefaultRoot(zk, zk.path, callback);
				},
				callback =>
				{
					ZookeeperMonitorUtility.CreateAllNode(zk, buffer, zookeeper.CreateMode.EPHEMERAL, err =>
					{
						if (err)
						{
							logger.error('create server node %s failed, with err : %j ', zk.nodePath, err.stack);
							utils.invokeCallback(callback, err);
							return;
						}
						utils.invokeCallback(callback);
					});
				},
				callback =>
				{

					ZookeeperMonitorUtility.GetData(zk, zk.cmdPath, (event) =>
					{
						ZookeeperMonitorUtility.WatchCommand(event, zk);
					},
					(err, data) =>
					{
						logger.debug('cmd err:%s data: %j', err, data);
						utils.invokeCallback(callback);
					});
				}
			],
			(err, rs) =>
			{
				utils.invokeCallback(cb, err);
			});
	}

	static GetClusterInfo(app, zk, servers)
	{
		let success = true;
		const results = {};
		if (!servers.length)
		{
			logger.error('get servers data is null.');
			return;
		}

		const latch = CreateCountDownLatch(servers.length, {timeout: constants.TIME.TIME_WAIT_COUNTDOWN}, () =>
		{
			if (!success)
			{
				logger.error('get all children data failed, with serverId: %s', app.serverId);
				return;
			}
			logger.info('cluster servers information: %j', results);
			app.replaceServers(results);
		});

		_.forEach(servers, server =>
		{
			(serverInfo =>
			{
				if (!utils.startsWith(serverInfo, constants.RESERVED.ZK_NODE_COMMAND))
				{
					ZookeeperMonitorUtility.GetData(zk, `${zk.path}/${serverInfo}`, null, (err, data) =>
					{
						if (err)
						{
							logger.error('%s get data failed for server %s, with err: %j', app.serverId, serverInfo, err.stack);
							latch.done();
							success = false;
							return;
						}
						const allInfo = JSON.parse(data);
						results[serverInfo.serverId] = allInfo.serverInfo;
						latch.done();
					});
				}
				else
				{
					latch.done();
				}
			})(server);
		});
	}

	static CreateDefaultRoot(zk, path, cb)
	{
		zk.client.exists(path, (err, stat) =>
		{
			if (err)
			{
				logger.error('zk check default root with error: %j', err);
				utils.invokeCallback(err);
			}
			if (stat)
			{
				utils.invokeCallback(cb);
			}
			else
			{
				ZookeeperMonitorUtility.CreateNode(zk.client, path, null, zookeeper.CreateMode.PERSISTENT, cb);
			}
		});
	}

	static CreateAllNode(zk, value, mode, cb)
	{
		async.series(
			[
				callback =>
				{
					logger.debug('ceate data path');
					ZookeeperMonitorUtility.CreateNode(zk.client, zk.nodePath, value, mode, callback);
				},
				callback =>
				{
					logger.debug('ceate command path');
					ZookeeperMonitorUtility.CreateNode(zk.client, zk.cmdPath, null, mode, callback);
				}
			],
			(err, rs) =>
			{
				logger.debug('create all node with callback');
				utils.invokeCallback(cb, err);
			});
	}

	static CreateNode(client, path, value, mode, cb)
	{
		logger.debug('create node with path: %s', path);
		client.create(path, value, mode, (err, result) =>
		{
			logger.debug('create node with result: %s', result);
			utils.invokeCallback(cb, err);
		});
	}

	static DisconnectHandle(zk)
	{
		zk.client.on('disconnected', () =>
		{
			logger.error('%s disconnect with zookeeper server.', zk.app.serverId);
			if (!zk.app.get(constants.RESERVED.STOP_FLAG))
			{
				ZookeeperMonitorUtility.Reconnect(zk);
			}
			else
			{
				logger.info('%s is forcely stopped by pomelo commander.', zk.app.serverId);
			}
		});
	}

	static WatchCommand(event, zk)
	{
		if (event.type !== 3)
		{
			logger.debug('command event ignore.');
			return;
		}
		ZookeeperMonitorUtility.GetData(zk, this.cmdPath, (event) =>
		{
			ZookeeperMonitorUtility.WatchCommand(event, zk);
		},
		(err, data) =>
		{
			logger.debug('cmd err:%s data: %j', err, data);
			commander.init(zk, data);
		});
	}

	static GetChildren(zk, fun, callBack)
	{
		zk.client.getChildren(zk.path, fun, (err, children, stats) =>
		{
			if (err)
			{
				utils.invokeCallback(callBack, err);
				return;
			}
			utils.invokeCallback(callBack, null, children);
		});
	}

	static Reconnect(zk)
	{
		if (zk.state === ZK_CONNECTING || zk.state === ZK_RECONNECTING)
		{
			logger.warn('zookeeper client is in invalid state.');
			return;
		}
		logger.info('%s server is reconnecting', zk.app.serverId);
		zk.state = ZK_RECONNECTING;
		let count = 0;
		let retry = true;
		const retries = zk.reconnectTimes;
		async.whilst(
			() =>
			{
				return count <= retries && retry;
			},
			next =>
			{
				count += 1;
				logger.debug('%s server is try to connect to zookeeper', zk.app.serverId);
				zk.client.close();
				zk.client = zookeeper.createClient(zk.servers, {
					sessionTimeout : zk.timeout,
					retries        : 0,
					spinDelay      : zk.spinDelay
				});

				zk.cbTimer = setTimeout(() =>
				{
					logger.info('reconnect to zookeeper timeout');
					zk.state = ZK_CONNECT_FAIL;
					utils.invokeCallback(next);
				}, constants.TIME.DEFAULT_ZK_CONNECT_TIMEOUT);

				zk.client.once('connected', () =>
				{
					logger.info('%s connect zookeeper successfully.', zk.app.serverId);
					zk.state = ZK_CONNECTED;
					zk.client.addAuthInfo('digest', new Buffer(zk.authentication));
					if (zk.setACL)
					{
						zk.client.setACL(zk.path, zk.acls, -1, (err, stat) =>
						{
							if (err)
							{
								logger.error('failed to set ACL: %j', err.stack);
								clearTimeout(zk.cbTimer);
								throw err;
							}
							clearTimeout(zk.cbTimer);
							ZookeeperMonitorUtility.RegisterZK(zk, () =>
							{
								ZookeeperMonitorUtility.GetAndWatchCluster(zk);
								ZookeeperMonitorUtility.DisconnectHandle(zk);
								retry = false;
							});
							logger.info('ACL is set to: %j', zk.acls);
						});
					}
					else
					{
						clearTimeout(zk.cbTimer);
						ZookeeperMonitorUtility.RegisterZK(zk, () =>
						{
							ZookeeperMonitorUtility.GetAndWatchCluster(zk);
							ZookeeperMonitorUtility.DisconnectHandle(zk);
							retry = false;
						});
					}
				});

				zk.state = ZK_RECONNECTING;
				zk.client.connect();
			},
			err =>
			{
				logger.info('ACL is err: %j', err);
			}
		);
	}
}

module.exports = function(app, opts)
{
	if (!(this instanceof ZookeeperMonitor))
	{
		return new ZookeeperMonitor(app, opts);
	}
};