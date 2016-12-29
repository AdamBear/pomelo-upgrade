/**
 * Created by frank on 16-12-26.
 */

const _ = require('lodash'),
	Redis = require('ioredis'),
	RedisClass = require('redis'),
	commander = require('./common/cmd'),
	utils = require('../util/Utils'),
	constants = require('../util/constants'),
	logger = require('pomelo-logger').getLogger('pomelo', __filename);

class RedisMonitor
{
	constructor(app, opts)
	{
		this.app = app;
		this.mode = opts.mode || 'single';
		this.name = opts.name || null;
		this.redisNodes = opts.redisNodes || [];
		this.period = opts.period || constants.TIME.DEFAULT_REDIS_REG;
		this.expire = opts.expire || constants.TIME.DEFAULT_REDIS_EXPIRE;
		this.password = opts.password || null;
		this.redisOpts = opts.redisOpts || {};
	}

	start(callBack)
	{
		if (this.mode === 'single')
		{
			this.client = new Redis(this.redisNodes.port, this.redisNodes.host, this.redisOpts);
		}
		else
		{
			this.client = new Redis(
				{
					sentinels : this.redisNodes.hosts,
					password  : this.password,
					name      : this.name
				}, this.redisOpts);
		}

		this.client.on('connect', () =>
		{
			logger.info('%s connected to redis successfully !', this.app.serverId);
			if (this.password)
			{
				this.client.auth(this.password);
			}
			RedisMonitorUtility.WatcherCluster2Command(this);
			this.timer = setInterval(() => {RedisMonitorUtility.WatcherCluster2Command(this);}, this.period);

			if (this.mode === 'multiple')
			{
				RedisMonitorUtility.ClearPingTimer(this, () =>
				{
					RedisMonitorUtility.GetMaster(this, this.redisNodes);
				});
			}

			utils.invokeCallback(callBack);
		});

		this.client.on('error', error =>
		{
			logger.error('[redisMonitor] server has errors with redis server, with error: %j', error);
		});

		this.client.on('close', () =>
		{
			logger.error('[redisMonitor] server has been closed with redis server.');
		});

		this.client.on('end', () =>
		{
			logger.error('[redisMonitor] server is over and without reconnection.');
		});
	}

	stop()
	{
		this.client.disconnect();
		if (this.pingRedis)
		{
			this.pingRedis.end();
		}
		clearInterval(this.timer);
	}

	sendCommandResult(result, type)
	{
		let key;
		if (!type)
		{
			// send result to redis, key:
			key = `${constants.RESERVED.REDIS_REG_RES_PREFIX + this.app.env}:${this.app.getCurServer().id}`;
		}
		else
		{
			// send show info to redis, key:
			key = `${constants.RESERVED.REDIS_REG_RES_PREFIX + this.app.env}:${this.app.getCurServer().id}:${type}`;
		}

		this.client.set(key, result, function(err)
		{
			if (err)
			{
				logger.error('set %j err: %j', key, err);
			}
		});
	}
}

class RedisMonitorUtility
{
	static WatcherCluster2Command(redisMonitor)
	{
		RedisMonitorUtility.GetClusterInfo(redisMonitor, redisMonitor.app, redisMonitor.client, redisMonitor.app.getCurServer());
		RedisMonitorUtility.GetCommand(redisMonitor, redisMonitor.app, redisMonitor.client, redisMonitor.app.getCurServer());
	}

	static GetMaster(redisMonitor, redisNodes)
	{
		logger.info('[redisMonitor] get master');
		const redis = redisMonitor.client;
		const clients = redisNodes.redis;
		const password = redisNodes.password;
		_.forEach(clients, client =>
		{
			((client) =>
			{
				const port = client.port;
				const host = client.host;
				
				logger.info('[redisMonitor] get master with, port: %s, host: %s', port, host);
				let redisClient = RedisClass.createClient(port, host, {auth_pass: password});
				redisClient.on('connect', function()
				{
					logger.info('[redisMonitor] connect redis host: %s port: %s successfully.', host, port);
					redisClient.info('replication', (err, info) =>
					{
						if (err)
						{
							logger.error('[redisMonitor] get redis info error with host: %s port: %s', host, port);
						}
						const obj = {};
						const lines = info.toString().split('\r\n');
						_.forEach(lines, line =>
						{
							const parts = line.split(':');
							if (parts[1])
							{
								obj[parts[0]] = parts[1];
							}
						});

						if (obj.role == 'master')
						{
							redisMonitor.pingRedis = client;
							redisMonitor.pingtimer = setInterval(() =>
							{
								logger.info('[redisMonitor] ping redis with host: %s port: %s', host, port);
								RedisMonitorUtility.Ping(redisMonitor, client, redis);
							}, constants.TIME.DEFAULT_REDIS_PING);
						}
						else
						{
							client.end();
							client = null;
						}
					});
				});
				redisClient.on('error', function()
				{
					logger.error('[redisMonitor] monitor redis connect error');
					redisClient.end();
					redisClient = null;
				});
				
			})(client);
		});
	}

	static ClearPingTimer(redisMonitor, callBack)
	{
		logger.info('[redisMonitor] clear ping timer');
		clearInterval(redisMonitor.pingtimer);
		let client = redisMonitor.pingRedis;
		if (client)
		{
			client.end();
			client = null;
			redisMonitor.pingtimer = null;
		}
		utils.invokeCallback(callBack);
	}
	
	static Ping(redisMonitor, client, redis)
	{
		const timeout = setTimeout(() =>
		{
			logger.error('[redisMonitor] ping redis timeout');
			clearInterval(redisMonitor.pingtimer);
			if (redisMonitor.pingtimer)
			{
				logger.info('[redisMonitor] clear pingtimer timeout');
				client.end();
				client = null;
				redisMonitor.pingtimer = null;
				redis.end();
				redis = null;
				redisMonitor.start(() =>
				{
					// 待填充内容
				});
			}
		}, constants.TIME.DEFAULT_REDIS_PING_TIMEOUT);
		client.ping(err =>
		{
			clearTimeout(timeout);
			if (err)
			{
				logger.error('[redisMonitor] redis ping error');
			}
			logger.info('[redisMonitor] ping');
		});
	}

	static GetCommand(redisMonitor, app, redis, serverInfo)
	{
		const key = `${constants.RESERVED.REDIS_REG_PREFIX + app.env}:${serverInfo.id}`;
		redis.get(key, function(err, res)
		{
			if (err)
			{
				logger.error('get pomelo-regist cmd err %j', err);
				return;
			}

			if (res)
			{
				logger.debug('get cmd: ', res);
				redis.del(key, function(err)
				{
					if (err)
					{
						logger.error('del command err %j', err);
					}
					else
					{
						commander.init(redisMonitor, res);
					}
				});
			}
		});
	}

	static GetClusterInfo(redisMonitor, app, redis, serverInfo)
	{
		const results = {};
		const key = constants.RESERVED.REDIS_REG_PREFIX + app.env;
		serverInfo.pid = process.pid;
		const args = [key, Date.now() + redisMonitor.expire, JSON.stringify(serverInfo)];

		redis.zadd(args, (err, res) =>
		{
			if (err)
			{
				logger.error('zadd %j err: %j', args, err);
				return;
			}

			const query_args = [key, Date.now(), '+inf'];
			redis.zrangebyscore(query_args, (err, res) =>
			{
				if (err)
				{
					logger.error('zrangebyscore %j err: %j', query_args, err);
					return;
				}

				for (let i = res.length - 1; i >= 0; i--)
				{
					const server = JSON.parse(res[i]);
					results[server.id] = server;
				}

				logger.info('cluster servers info: %j', results);
				app.replaceServers(results);
			});
		});
	}
}

module.exports = function(app, opts)
{
	if (!(this instanceof RedisMonitor))
	{
		return new RedisMonitor(app, opts);
	}
};