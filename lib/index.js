'use strict';
var config = require('ih-config');
var log = require('ih-log');
var rp = require('request-promise');
var _ = require('lodash');
var uuid = require('uuid');

var TTL_IN_MIN = 3;

//TODO: make this optional/overridable from callers.
//eliminates extra data being returned from ES
var filterPath ='&filter_path=_scroll_id,took,hits.hits,hits.hits._id,hits.hits._source,hits.total,aggregations.*';

var headers = {
  "Content-Type": "application/json"
};
if (config.get('elastic:username') && config.get('elastic:password')){
  headers["authorization"] = 'Basic ' + new Buffer(config.get('elastic:username') + ':' + config.get('elastic:password')).toString('base64')
}

module.exports = {
  executeSearch: executeSearch,
  executeCount: executeCount,
  executeScroll: executeScroll,
  executeBasicGet: executeBasicGet,
  pageScroll: pageScroll,
  scrollToEnd: scrollToEnd,
  parseScrollId: parseScrollId
};

/**
* Sends a query to elasticsearch _search endpoint for given index and type.
* @param  {string} index       Elasticsearch index used.
* @param  {string} type        Elasticsearch type to query.
* @param  {string} body        Elasticsearch query body. String-form JSON.
* @param  {string} queryString Querystring to append to _search. Include the '?'.
* @param  {string} logMessage  Optional logging message.
* @return {Promise(object)}    Parsed JSON response from elastic.
*/
function executeSearch(index, type, body, logMessage, queryString) {
  var searchId = uuid.v1();
  queryString = (queryString || '') + (queryString ? '&' : '?') + 'request_cache=true' + filterPath;
  var path = index + '/' + type + '/_search' + queryString;
  var profileMessage = 'ES ' + index + ' _search ' + logMessage + ' ' + searchId;
  profileSearch();
  return httpPost(path, body, logMessage)
    .then(profileSearch);

  function profileSearch(resp){
    if (resp) {
      log.debug(profileMessage + ' timeonES=' + resp.took + 'ms');
    }
    log.debug(profileMessage);
    return resp;
  }
}

/**
* Sends a count query to elasticsearch _search endpoint for given index and type.
* Automatically appends ?search_type=count.
* *** Will include any aggs that are part of the query. ***
* @param  {string} index       Elasticsearch index used.
* @param  {string} type        Elasticsearch type to query.
* @param  {string} body        Elasticsearch query body. String-form JSON.
* @param {string} logMessage   optional log message to identify this query in the logs.
* @return {Promise(object)}    Parsed JSON response from elastic.
*/
function executeCount(index, type, body, logMessage) {
  logMessage = logMessage || 'executeCount';
  return executeSearch(index, type, body, logMessage, '?search_type=count');
}

/**
* Sends a query to elasticsearch _search endpoint for given index and type.
* Automatically appends ?scroll=1m&search_type=scan unless noScan is set.
*
* *** Will include any aggs that are part of the query. ***
* @param  {string} index       Elasticsearch index used.
* @param  {string} type        Elasticsearch type to query.
* @param  {string} body        Elasticsearch query body. String-form JSON.
* @param  {int} ttlInMin   Time for scroll to live between requests in minutes.
* @param  {boolean} noScan     If true, don't use scan. Defaults to false.
* @return {Promise(object)}    Parsed JSON response from elastic. -Includes _scroll_id for pageScroll calls.
*/
function executeScroll(index, type, body, ttlInMin, noScan) {
  TTL_IN_MIN = ttlInMin || TTL_IN_MIN;
  var ttlString = TTL_IN_MIN + 'm';
  var queryString = '?scroll=' + ttlString;

  log.debug('ttlInMin', TTL_IN_MIN);
  log.debug('queryString', queryString);
  return executeSearch(index, type, body, 'executeScroll', queryString);
}

/**
* Wraps scroll/scan methods.
*
* *** Will include any aggs that are part of the query. ***
* @param  {string} index       Elasticsearch index used.
* @param  {string} type        Elasticsearch type to query.
* @param  {string} body        Elasticsearch query body. String-form JSON.
* @param  {int} ttlInMin       Time for scroll to live between requests in minutes.
* @param  {boolean} noScan     If true, don't use scan. Defaults to false.
* @return {Promise(object)}    Parsed JSON response from elastic. -Includes _scroll_id for pageScroll calls.
*/
function scrollToEnd (index, type, body, ttlInMin, noScan) {
  var recurseScroll = _.curry(scroll)([]);
  return executeScroll(index, type, body, ttlInMin, noScan)
    .then(recurseScroll);
}

/**
* Gets the scroll results for the provided scrollId
*
* *** Will include any aggs that are part of the query. ***
* @param  {string} scrollId    Elasticsearch scroll_id to get the next page for.
* @return {Promise(object)}    Parsed JSON response from elastic. -Includes _scroll_id for pageScroll calls.
*/
function pageScroll(scrollId){
  var path = '/_search/scroll?scroll=' + TTL_IN_MIN + 'm&scroll_id=' + (scrollId || '') + filterPath;
  profileScroll();
  return httpGet(path)
    .tap(profileScroll);

  function profileScroll(){
    log.debug('scroll ...' + scrollId);
  }
}

function parseScrollId(res) {
  if (!res._scroll_id) {
    throw new Error('no scroll id on scroll response.');
  }
  return res._scroll_id;
}

/**
* Executes a basic GET request at the given path.  Search requests will throw an error.
* Meant for reaching admin endpoints such as '/', the cat apis, the cluster apis, etc.
*
* @param {string} path       The endpoint path
* @param {boolean} verbose   Specifies whether or not to add '?v' to request, which enables a verbose response on any request
* @return {Promise(object)}  Parsed JSON response from elastic
*/
function executeBasicGet(path, verbose) {
    if (path.toLowerCase().includes('_search')) {
        throw new Error('Basic Get cannot perform search.');
    }
    if (verbose) path = path + '?v';

    return httpGet(path);
}

/**
* Private - Not exported below this line.
*/
function scroll(results, res){
    results = results.concat(res.hits.hits);
    var curriedScroll = _.curry(scroll)(results);
    if (results.length < res.hits.total) {
        if (res.hits.hits.length === 0){
            throw new Error('Scroll request timed out');
        }
        return pageScroll(parseScrollId(res))
            .then(curriedScroll);
    }
    else {
        return results;
    }

}

function httpGet(path) {
    log.debug('Executing elastic get route: ...[%s]', path.substr(path.length-5, 5));
    const options = buildHttpOptions('GET', path);
    return rp(options);
}

function httpPost(path, body, logMessage) {
    logMessage = typeof logMessage !== 'undefined' ? logMessage : '';
    log.debug('%s Executing elastic query route: [%s] body: %s', logMessage, path, body);
    const options = buildHttpOptions('POST', path, body);
    return rp(options);
}

function buildHttpOptions(method, path, body) {
  const options = {
      uri: buildUri(path),
      method: method,
      headers: headers
  };

  if(config.get('elastic:ca')){
    options.agentOptions = {
      ca: config.get('elastic:ca')
    }
  }

  if(method === 'POST'){
    options.body = body;
  }

  return options;
}

function buildUri(path){
  const protocol = (config.get('elastic:ca') ? 'https://' : 'http://');
  return protocol.concat(config.get('elastic:server')).concat(':').concat(config.get('elastic:port')).concat('/').concat(path);
}

