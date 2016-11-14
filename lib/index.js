'use strict';
var config = require('ih-config');
var Promise = require('bluebird');
var log = require('ih-log');
var http = require('q-io/http');
var _ = require('lodash');
var uuid = require('uuid');

var TTL_IN_MIN = 3;

//TODO: make this optional/overridable from callers.
//eliminates extra data being returned from ES
var filterPath ='&filter_path=_scroll_id,took,hits.hits,hits.hits._id,hits.hits._source,hits.total,aggregations.*';

var headers = {};
if (config.get('elastic:username') && config.get('elastic:password')){
  headers = {
    "authorization" : 'Basic ' + new Buffer(config.get('elastic:username') + ':' + config.get('elastic:password')).toString('base64')
  };
}

module.exports = {
  executeSearch: executeSearch,
  executeCount: executeCount,
  executeScroll: executeScroll,
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
    .then(function(resp){return parseElasticResponse(resp, profileMessage);})
    .then(profileSearch);

  function profileSearch(resp){
    if (resp) {
      log.info(profileMessage + ' timeonES=' + resp.took + 'ms');
    }
    log.profile(profileMessage);
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

  log.info('ttlInMin', TTL_IN_MIN);
  log.info('queryString', queryString);
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
function pageScroll(scrollId, rawData){
  var path = '/_search/scroll?scroll=' + TTL_IN_MIN + 'm&scroll_id=' + (scrollId || '') + filterPath;
  var parse = rawData ? readRawBytes : parseElasticResponse;
  profileScroll();
  return Promise.resolve(httpGet(path))
    .then(parse)
    .tap(profileScroll);

  function profileScroll(){
    log.profile('scroll ...' + scrollId);
  }
}

function parseScrollId(res) {
  if (!res._scroll_id) {
    throw new Error('no scroll id on scroll response.');
  }
  return res._scroll_id;
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
  return http.request({
    host: config.get('elastic:server'),
    port: config.get('elastic:port'),
    path: path,
    method: 'GET',
    headers: headers
  });
}


function httpPost(path, body, logMessage) {
  logMessage = typeof logMessage !== 'undefined' ? logMessage : '';
  log.debug('%s Executing elastic query route: [%s] body: %s', logMessage, path, body);
  return http.request({
    host: config.get('elastic:server'),
    port: config.get('elastic:port'),
    path: path,
    method: 'POST',
    body: [body],
    headers: headers
  });
}

function parseElasticResponse(response, profileMessage) {
  if (profileMessage) {
    log.profile(profileMessage + ' READ');
  }
  return response.body.read()
  .then(function(body) {
    if (profileMessage) {
      log.profile(profileMessage + ' READ');
    }
    if (profileMessage) {
      log.profile(profileMessage + ' PARSE' );
    }
    var str = body.toString('utf-8');
    //log.debug('ES response from %s: %s', profileMessage, str);
    //NOTE: eval is only used here because we trust the source (Elasticsearch)
    var resp = eval('(' + str + ')'); // jshint ignore:line
    if (profileMessage) {
      log.profile(profileMessage + ' PARSE');
    }
    return resp;
  });
}

function readRawBytes(response) {
  return response.body.read()
  .then(function(response) {
    return response.toString();
  });
}

function getHash(s) {
  var hash = 0,
    i, char;
  if (s.length === 0) return hash;
  for (i = 0; i < s.length; i++) {
    char = s.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}
