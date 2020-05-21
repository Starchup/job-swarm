/**
 * Applies filter object to the googleQueryObject
 *
 * Filters have the following format:
 *	{
		where: {
			property: value,
			property: {
				comparator: value
				//Any properties after the first comparator will be ignored.
			}
		},
		order: value,
		limit: value
 	}
 *	Where filter only supports 'and'.  Supported comparators are: 'lt', 'lte', 'gt', 'gte', 'eq'
 *
 */
function mapFilter(googleQueryObject, filter)
{
	if (filter.where)
	{
		const query = filter.where;

		for (let prop in query)
		{
			if (prop === 'kind') continue;
			let comparator = '=';
			let value = query[prop];
			if (getType(value) === 'Object')
			{
				const firstKey = Object.keys(value)[0];
				comparator = comparison(firstKey);
				value = value[firstKey];
			}
			googleQueryObject = googleQueryObject.filter(prop, comparator, value);
		}
	}

	if (filter.order) googleQueryObject = googleQueryObject.order(filter.order);
	if (filter.hasOwnProperty('limit')) googleQueryObject = googleQueryObject.limit(filter.limit);

	return googleQueryObject;
}

//Prepare entity in Google Datastore expected format
function makeEntity(data, maxLength)
{
	const ent = [];
	for (let key in data)
	{
		if (data[key] && data[key].length >= maxLength)
		{
			ent.push(
			{
				name: key,
				value: data[key],
				excludeFromIndexes: true
			});
		}
		else
		{
			ent.push(
			{
				name: key,
				value: data[key]
			});
		}
	}
	return ent;
}

function newOpenJob(name, type, data)
{
	let jobData = {
		name: name,
		status: 'open',
		created: new Date().toJSON(),
		environment: process.env.NODE_ENV || 'testing',
		startTime: null,
		completionTime: null,
		error: null,
	};

	if (type) jobData.type = type;
	if (data) jobData.data = data;

	return jobData;
}

function resultCount(res)
{
	let count = 0;
	if (res && res[0] && res[0].mutationResults && res[0].mutationResults.length)
	{
		count = res[0].mutationResults.length;
	}
	return {
		count: count
	};
}

//Returns a string'd comparison operator for a query comparison
function comparison(val)
{
	if (!val) return '=';
	const comparatorDict = {
		'lt': '<',
		'lte': '<=',
		'gt': '>',
		'gte': '>=',
		'eq': '=',
	};
	const comparator = comparatorDict[val];
	if (!comparator) throw new Error(`Comparator '${val}' is not supported. Supports 'lt', 'lte', 'gt', 'gte', 'eq'`);
	return comparator;
}

function getType(val)
{
	return Object.prototype.toString.call(val).slice(8, -1);
}

module.exports = {
	mapFilter,
	makeEntity,
	newOpenJob,
	resultCount,
	comparison,
	getType
};