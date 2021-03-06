const
{
    Datastore
} = require('@google-cloud/datastore');
const moment = require('moment');

const
{
    mapFilter,
    makeEntity,
    newOpenJob,
    resultCount,
    comparison,
    getType
} = require('./utilities');

const
{
    checkContext,
    typeError,
    permissionError
} = require('./errors');


class JobSwarm
{
    constructor(options)
    {
        const defaultOptions = {
            projectId: undefined,
            namespace: undefined,
            apiEndpoint: undefined,
            entityKind: 'Job',
            controller: false,
            staleAfter: '',
            maxFieldLength: 1000000,
            maxIndexLength: 1500,
            maxDeleteLength: 500
        };
        this.config = Object.assign(
        {}, defaultOptions, options);


        //Check required settings
        const requiredOptions = {
            projectId: 'String',
            namespace: 'String',
            entityKind: 'String',
        };
        for (let prop in requiredOptions)
        {
            if (!this.config[prop] || getType(this.config[prop]) !== requiredOptions[prop])
            {
                throw new Error(`Property ${prop} is required and must be type ${requiredOptions[prop]}. Actual: ${this.config[prop]}`);
            }
        }

        //Initialize datastore and make available
        this.datastore = new Datastore(
        {
            projectId: this.config.projectId,
            apiEndpoint: this.config.apiEndpoint,
            namespace: this.config.namespace,
        });
    }

    getJobByKey(key)
    {
        return this.datastore.get(key).then(res => res[0]);
    }

    //Get the earliest open job
    //Optionally allow to filter by type (user-defined), or kind (datastore type)
    getNextJob(name, type, lastJobOnly)
    {
        const filter = {
            where:
            {
                status: 'open'
            }
        };
        if (name) filter.where.name = name;
        if (type) filter.where.type = type;

        if (lastJobOnly)
        {
            filter.order = 'created';
            filter.limit = 1;
        }

        let queryObj = this.datastore.createQuery(this.config.namespace, this.config.entityKind);
        queryObj = mapFilter(queryObj, filter);
        return this.datastore.runQuery(queryObj).then(res =>
        {
            if (lastJobOnly) return res[0];
            else return res[0].sort((a, b) =>
            {
                return new Date(a.date) - new Date(b.date);
            })[0];
        });
    }

    getExistingActiveJobs(name, type)
    {
        const filter = {
            where:
            {
                status: 'active'
            }
        };

        if (name) filter.where.name = name;
        if (type) filter.where.type = type;

        let queryObj = this.datastore.createQuery(this.config.namespace, this.config.entityKind);
        queryObj = mapFilter(queryObj, filter);
        return this.datastore.runQuery(queryObj).then(res => res[0]);
    }

    //Optionally allow custom kind (datastore type).  Fallback to default kind, set on initialization
    createOpenJob(name, type, data, splitBy)
    {
        const nameType = getType(name);
        const typeType = getType(type);
        if (nameType !== 'String') typeError('Job name', 'String', nameType);
        if (type && typeType !== 'String') typeError('Job type', 'String', typeType);

        const jobData = [];
        if (!data || data.length < this.config.maxFieldLength)
        {
            jobData.push(newOpenJob(name, type, data));
        }
        else
        {
            // If the name is greater than the max field limit
            // then lower it back down to manageable chunks
            while (data.length > 0)
            {
                var splitData = data.split(splitBy || '\n');

                var d = '';
                do {
                    d += splitData.shift() + (splitBy || '\n');
                } while (splitData.length > 0 && d.length < this.config.maxFieldLength);

                d = d.substring(d.length);
                jobData.push(newOpenJob(name, type, d));
            }
        }

        return saveEntity.call(this, jobData);
    }

    activateJob(job)
    {
        return updateEntity.call(this, job,
        {
            status: 'active',
            startTime: new Date().toJSON(),
        });
    }

    completeJob(job)
    {
        return updateEntity.call(this, job,
        {
            status: 'complete',
            completionTime: new Date().toJSON(),
        });
    }

    completeJobWithError(job, error)
    {
        //Handle different error formats
        let stringError;
        if (error instanceof Error) stringError = JSON.stringify(error.stack);
        else if (getType(error) === 'String') stringError = error;
        else stringError = JSON.stringify(error);

        return updateEntity.call(this, job,
        {
            status: 'failed',
            error: stringError,
            completionTime: new Date().toJSON(),
        });
    }

    skipJob(job)
    {
        return updateEntity.call(this, job,
        {
            status: 'skipped',
            completionTime: new Date().toJSON(),
        });
    }

    markJobStale(job)
    {
        return updateEntity.call(this, job,
        {
            status: 'stale'
        });
    }

    reserveJob(job, reservationName)
    {
        return updateEntity.call(this, job,
        {
            status: 'reserved',
            reservation: reservationName,
            reservationTime: new Date().toJSON(),
        });
    }

    /* Controller-permission methods */

    /**
     * Takes filter with properties {where:..., order:..., limit:...}
     * OR logic is supported via where: {or: [condition1, condition 2]}
     * With OR logic, order and limit applies to each OR condition, not the results as a whole
     * Results may include duplicates
     */
    getJobs(filter)
    {
        if (!this.config.controller) permissionError();

        //Handle OR logic
        let filterArray;
        if (filter && filter.where && filter.where.or)
        {
            if (getType(filter.where.or) !== 'Array') typeError('filter.where.or', 'Array', getType(filter.where.or));

            filterArray = filter.where.or.map(q =>
            {
                const f = {
                    where: q,
                };
                if (filter.order) f.order = filter.order;
                if (filter.limit) f.limit = filter.limit;
                return f;
            });

        }

        if (filterArray)
        {
            let results = [];
            return filterArray.reduce((prev, f) =>
            {
                return prev.then(() =>
                {
                    const fType = getType(f);
                    if (fType !== 'Object') typeError('Query object', 'Object', fType);
                    return query.call(this, f);
                }).then(res =>
                {
                    if (res && res.length) results = results.concat(res);
                });
            }, Promise.resolve()).then(() =>
            {
                return results;
            });
        }
        else
        {
            const fType = getType(filter);
            if (filter && fType !== 'Object') typeError('Query object', 'Object', fType);
            return query.call(this, filter);
        }
    }

    getStaleJobs(type)
    {
        if (!this.config.controller) permissionError();
        if (!this.config.staleAfter || !this.config.staleAfter.trim()) throw new Error('getStaleJobs requires the controller to be instantiated with a staleAfter value');

        if (!this.config.staleAfterReserved || !this.config.staleAfterReserved.trim()) throw new Error('getStaleJobs requires the controller to be instantiated with a staleAfterReserved value');


        const [staleTime, staleUnit] = this.config.staleAfter.split(' ');
        const [staleReservedTime, staleReservedUnit] = this.config.staleAfterReserved.split(' ');

        if (!staleTime || !staleUnit) throw new Error(`config.staleAfter must be in format \`\$\{value} \$\{unit}\`. Actual: ${this.config.staleAfter}`);

        if (!staleReservedTime || !staleReservedUnit) throw new Error(`config.staleAfterReserved must be in format \`\$\{value} \$\{unit}\`. Actual: ${this.config.staleAfterReserved}`);

        const activeCutoff = moment().subtract(staleTime, staleUnit);
        if (!activeCutoff.isValid()) throw new Error('Invalid date');

        const reservedCutoff = moment().subtract(staleReservedTime, staleReservedUnit);
        if (!reservedCutoff.isValid()) throw new Error('Invalid date');

        const filter = {
            where:
            {
                or: [
                {
                    status: 'active',
                    startTime:
                    {
                        lt: activeCutoff.toJSON()
                    }
                },
                {
                    status: 'reserved',
                    reservationTime:
                    {
                        lt: reservedCutoff.toJSON()
                    }
                }]
            }
        };
        if (type)
        {
            filter.where.or[0].type = type;
            filter.where.or[1].type = type;
        }
        return this.getJobs(filter);
    }

    //Takes array of {name: '', type: ''} objects
    createJobs(data, splitBy)
    {
        if (!this.config.controller) permissionError();
        if (getType(data) !== 'Array') typeError('Jobs data', 'Array', getType(data));

        return data.reduce((prev, d) =>
        {
            return prev.then(() =>
            {
                return this.createOpenJob(d.name, d.type, d.data, splitBy);
            });
        }, Promise.resolve());
    }

    //Allows controller to create complete custom entity
    createRawEntity(data)
    {
        if (!this.config.controller) permissionError();
        return saveEntity.call(this, data);
    }

    deleteJob(job)
    {
        if (!this.config.controller) permissionError();
        return this.datastore.delete(job[this.datastore.KEY]).then(resultCount);
    }

    deleteJobs(filter)
    {
        if (!this.config.controller) permissionError();

        return this.getJobs(filter).then(jobs =>
        {
            const jobKeysToDelete = jobs.map(j => j[this.datastore.KEY]);
            if (!jobKeysToDelete || !jobKeysToDelete.length) return Promise.resolve(
            {
                count: 0
            });

            let chunkedJobsToDelete = [];
            while (jobKeysToDelete.length > 0)
            {
                chunkedJobsToDelete.push(jobKeysToDelete.splice(0, this.config.maxDeleteLength));
            }
            return chunkedJobsToDelete.reduce((prev, jobs) =>
            {
                return prev.then(() =>
                {
                    return this.datastore.delete(jobs).then(resultCount);
                });
            }, Promise.resolve());
        });
    }
}


/* Private methods - must be called with bind/call/apply to have appropriate 'this' context */

function query(filterObj)
{
    let queryObj = this.datastore.createQuery(this.config.namespace, this.config.entityKind);
    if (filterObj) queryObj = mapFilter(queryObj, filterObj);
    return this.datastore.runQuery(queryObj).then(res => res[0]);
}



/**
 * Wrapper for creating datastore entities
 * Data argument may be object or array of objects
 */
function saveEntity(data)
{
    checkContext('saveEntity', this);

    if (getType(data) !== 'Array') data = [data];

    let entity = data.map(d =>
    {
        const dataType = getType(d);
        if (dataType !== 'Object') typeError('Enity data', 'Object', dataType);
        return prepareEntities.call(this, d);
    });

    if (entity.length === 1) entity = entity[0];
    return this.datastore.save(entity).then(resultCount);
}

function updateEntity(entity, data)
{
    checkContext('updateEntity', this);

    if (!entity) return Promise.reject('entity required for update');

    const key = entity[this.datastore.KEY];

    //Datastore updates are full updates - must use transactions to get
    //and then update, or else data will be overwritten
    const transaction = this.datastore.transaction();

    return transaction.run().then(() =>
    {
        return this.datastore.get(key);
    }).then(res =>
    {
        if (!res || !res[0]) throw new Error(`Entity with key ${JSON.stringify(key)} does not exist`);
        const currentEntity = res[0];
        const updatedEntity = {
            key: key,
            data: makeEntity(Object.assign(
            {}, currentEntity, data), this.config.maxIndexLength)
        };
        transaction.save(updatedEntity);
        return transaction.commit();
    }).then(resultCount).catch(e =>
    {
        return transaction.rollback().then(() =>
        {
            throw e;
        });
    });
}

function prepareEntities(data)
{
    checkContext('prepareEntities', this);

    delete data.kind;

    return {
        key: this.datastore.key(this.config.entityKind),
        data: makeEntity(data, this.config.maxIndexLength),
    };
}

module.exports = JobSwarm;