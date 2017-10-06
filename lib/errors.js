/* Error utilities */
function checkContext(name, self)
{
	if (!self || self.config === undefined) throw new Error(`Attempting to call ${name} out of context, "this" context incorrect`);
}

function typeError(name, expected, actual)
{
	throw new Error(`${name} must be type ${expected}. Actual: ${actual}`);
}

function permissionError()
{
	throw new Error(`This method requires instance to be initialized with 'controller: true'`);
}

module.exports = {
	checkContext,
	typeError,
	permissionError
};