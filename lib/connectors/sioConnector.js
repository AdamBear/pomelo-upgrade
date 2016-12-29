const EventEmitter = require('events').EventEmitter,
	sio = require('socket.io'),
	SioSocket = require('./sio/sioSocket');

const PKG_ID_BYTES = 4;
const PKG_ROUTE_LENGTH_BYTES = 1;
const PKG_HEAD_BYTES = PKG_ID_BYTES + PKG_ROUTE_LENGTH_BYTES;

let curId = 1;

/**
 * Connector that manager low level connection and protocol bewteen server and client.
 * Develper can provide their own connector to switch the low level prototol, such as tcp or protoBuff.
 */
class SioConnector extends EventEmitter
{
	constructor(port, host, opts)
	{
		super();
		this.port = port;
		this.host = host;
		this.opts = opts;
		this.closeTimeout = opts.closeTimeout || 60;
		this.heartbeatTimeout = opts.heartbeatTimeout || 60;
		this.heartbeatInterval = opts.heartbeatInterval || 25;
	}

	/**
	 * Start connector to listen the specified port
	 */
	start(callBack)
	{
		// issue https://github.com/NetEase/pomelo-cn/issues/174
		if (this.opts)
		{
			this.wsocket = sio.listen(this.port, this.opts);
		}
		else
		{
			this.wsocket = sio.listen(this.port, {
				transports : ['websocket', 'polling']
			});
		}
		this.wsocket.set('heartbeat timeout', this.heartbeatTimeout * 1000);
		this.wsocket.set('heartbeat interval', this.heartbeatInterval * 1000);
		this.wsocket.sockets.on('connection', socket =>
		{
			const sioSocket = new SioSocket(curId++, socket);
			this.emit('connection', sioSocket);
			sioSocket.on('closing', reason =>
			{
				sioSocket.send({
					route  : 'onKick',
					reason : reason});
			});
		});

		process.nextTick(callBack);
	}

	/**
	 * Stop connector
	 */
	stop(force, callBack)
	{
		this.wsocket.server.close();
		process.nextTick(callBack);
	}
}

class SioConnectorUtility
{
	static EnCode(reqId, route, msg)
	{
		if (reqId)
		{
			return SioConnectorUtility.ComposeResponse(reqId, route, msg);
		}
		return SioConnectorUtility.ComposePush(route, msg);
	}

	/**
	 * Decode client message package.
	 *
	 * Package format:
	 *   message id: 4bytes big-endian integer
	 *   route length: 1byte
	 *   route: route length bytes
	 *   body: the rest bytes
	 *
	 * @param  {String} msg socket.io package from client
	 * @return {Object}      message object
	 */
	static DeCode(msg)
	{
		let index = 0;

		const id = SioConnectorUtility.ParseIntField(msg, index, PKG_ID_BYTES);
		index += PKG_ID_BYTES;

		const routeLen = SioConnectorUtility.ParseIntField(msg, index, PKG_ROUTE_LENGTH_BYTES);

		const route = msg.substr(PKG_HEAD_BYTES, routeLen);
		const body = msg.substr(PKG_HEAD_BYTES + routeLen);

		return {
			id    : id,
			route : route,
			body  : JSON.parse(body)
		};
	}

	static ComposeResponse(msgId, route, msgBody)
	{
		return {
			id   : msgId,
			body : msgBody
		};
	}

	static ComposePush(route, msgBody)
	{
		return JSON.stringify({
			route : route,
			body  : msgBody});
	}

	static ParseIntField(str, offset, len)
	{
		let res = 0;
		for (let i = 0; i < len; i++)
		{
			if (i > 0)
			{
				res <<= 8;
			}
			res |= str.charCodeAt(offset + i) & 0xff;
		}
		return res;
	}
}

SioConnector.encode = SioConnector.prototype.encode = SioConnectorUtility.EnCode;
SioConnector.decode = SioConnector.prototype.decode = SioConnectorUtility.DeCode;

module.exports = function(port, host, opts)
{
	if (!(this instanceof SioConnector))
	{
		return new SioConnector(port, host, opts);
	}
};

