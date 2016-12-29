/**
 * Filter for toobusy.
 * if the process is toobusy, just skip the new request
 */
const logger = require('pomelo-logger').getLogger('con-log', __filename);
let toobusy = null;
const DEFAULT_MAXLAG = 70;

const Filter = function(maxLag)
{
	try
	{
		toobusy = require('toobusy');
	}
	catch (e)
	{
		// null
	}
	if (toobusy)
	{
		toobusy.maxLag(maxLag);
	}
};

Filter.prototype.before = function(msg, session, next)
{
	if (toobusy && toobusy())
	{
		logger.warn(`[toobusy] reject request msg: ${msg}`);
		const err = new Error('Server toobusy!');
		err.code = 500;
		next(err);
	}
	else
	{
		next();
	}
};

module.exports = function(maxLag)
{
	return new Filter(maxLag || DEFAULT_MAXLAG);
};