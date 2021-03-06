var stream = require('stream');
var util = require('util');

function Encoder(options) {
	if (!options) options = {};
	options.objectMode = true;
	
	if (!(this instanceof Encoder)) {
		return new Encoder(options);
	}
	
	stream.Transform.call(this, options);
	// set properties here, this.foo = ...
}
util.inherits(Encoder, stream.Transform);

Encoder.prototype._transform = function(obj, enc, cb) {
	var self = this;
	
	if (!obj) {
		this.push(null);
		cb();
		return;
	}
	
	// process obj here (e.g. make primary key to arango _key)
	obj._key = "" + obj.findex;

	this.push(JSON.stringify(obj, function(key, value) {
			if (value == null || // ignore NULL'd values
				key === "findex" || // ignore a certain key
				(typeof value === "string" && !value.trim())) // ignore if whitespace only
			{
				return undefined;
			} else {
				return value;
			}
		}
	) + "\n");
	// important: arangoimp expects line break after document,
	// or it will assume an array otherwise (and import thus fail).
	// string cancat is 4-5% faster than a second call to push().
	cb();
};

var exports = module.exports = {stream: Encoder};
