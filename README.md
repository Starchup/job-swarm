#Job Swarm

Allows a server to become worker instance as part of a job swarm, using Google Datastore to track job status

### Initialization

```
const JobSwarm = require('job-swarm');
const swarm = new JobSwarm(
{
	projectId: YOUR_GOOGLE_CLOUD_PROJECT,
	apiEndpoint: API_ENDPOINT, //For emulated/hosted datastore
	namespace: YOUR_DATASTORE_NAMESPACE,
	entityKind: 'Job', //Google datastore kind.  Defaults to 'Job',
	controller: false,//true creates a controller instance, which has access to more methods for maintaining the datastore
});
```
Read more about Google Datastore: https://cloud.google.com/datastore/docs/


### Basic use

A pattern for running a job, and then updating status on completion/failure could look like this:

```
function runJobs(swarm)
{
	return swarm.getNextJob().then(job =>
	{
		if (!job) return Promise.resolve();

		return processJob(job);
	}).catch(console.error);

	function processJob(job)
	{
		//Set job status 'active' so other jobs will ignore
		return swarm.activateJob(job).then(() =>
		{
			
			/* Here is your job process function */
			return new Promise((resolve, reject) =>
			{
				//Some async function
				setTimeout(() =>
				{
					return resolve();
				}
				}, 5000);
			});
		}).then(() =>
		{
			//Job success (or at least not critical failure)
			//Job status 'complete'
			return swarm.completeJob(job);
		}).catch(e =>
		{
			//Job failed, mark the error
			//Job status 'failed'
			return swarm.completeJobWithError(job, e);
		}).then(() =>
		{
			//Replace completed job with another version of itself
			//To be handled by another job swarm instance
			return swarm.createOpenJob(job.name, job.type);
		}).catch(e =>
		{
			//Failed to make a new job
			console.error(e);
		});
	}
}

```

### API

The following methods are available for all instances of JobSwarm (options.controller = true/false)

* `getKey(job)`  Takes datastore entity and returns datastore key for that entity
* `getJobByKey(key)`  Takes datastore key and returns datastore entity
* `getNextJob(type, kind)` Takes optional type and key args.  Returns next job with status open of that type and kind

* `createOpenJob(name, type, kind)` Takes name, optional type and key args.  Creates job with status open. Returns count object.
* `activateJob(job)` Takes datastore job entity and sets status to 'active', startTime to new Date().toJSON()
* `completeJob(job)` Takes datastore job entity and sets status to 'complete', completionTime to new Date().toJSON()
* `completeJobWithError(job, err)` Takes datastore job entity and err and sets status to 'failed', completionTime to new Date().toJSON(), and error to err.message
* `skipJob(job)` Takes datastore job entity and sets status to 'skipped', completionTime to new Date().toJSON()

The following methods are available for controller instances of JobSwarm (options.controller = true)

* `getJobs(filter)`  Takes filter object `{where:..., order:..., limit:...} and returns matching jobs
* `createJobs(data)` Takes array of {name: '', type: ''} objects and creates jobs.  Returns count object
* `createRawEntity(data)` Takes data object and creates a completely custom datastore entity. Returns count object
* `deleteJob(job)` Takes job datastore entity and deletes. Returns count object
* `deleteJobs(filter)` Deletes job datastore entities matching filter. Returns count object


### Datastore Indexes
Google Datastore requires manual indexes to be set up for complex queries (queries with more than one equality or sort order, or queries with one or more equalities and one or more sort orders).  The only complex query built-in to this module is the query for .getNextJob().  Use the following as the basis for your index.yaml file for creating your datastore indexes, but you may need to add more if you decide to use more complex queries.

```
#index.yaml
indexes:

- kind: "Job"
  properties:
  - name: "status"
  - name: "created"
```

