'use strict';

var argv        = require('optimist').argv;
var fs          = require('fs');
var path        = require('path');
var http        = require('http');
var colors      = require('colors');
var moment      = require('moment');
var util        = require('util');
var http        = require('http');
var _           = require('underscore');

exports.VERSION = '0.0.1';

exports.Crawler = function (options) {
    var self = this;

    // Default options
    self.options = _.extend({
        solrUrl:        '',
        solrQuery:      '',
        solrSort:       '',
        solrFields:     '',
        NUM_ROWS:       20,
        MAX_DOCS:       500,
        current:        0,
        waitTime:       0,
        waitTimeBlock:  2000,
        cursorMark:     '*',
        onData:         function () {},
        onError:        function () {},
        onFinish:       function () {}
    }, options);

    // TODO: Check the parameters.

    var getDocumentsFromSolr = function (callback, retry) {
        util.puts('\nStarted process for documents between '.blue.bold + (self.options.current + 1).toString().yellow.bold + ' To '.blue.bold + (self.options.current + self.options.NUM_ROWS).toString().green.bold + ' (' + self.options.cursorMark + ')');

        retry = retry || 0;
        if (retry > 0) {
          console.log(util.format('Retrying to get the documents to process from Solr (retry=%d)', retry));
        }

        var url = util.format('%s?q=%s', self.options.solrUrl, self.options.solrQuery);
        url += util.format('&rows=%s', self.options.NUM_ROWS);
        url += util.format('&fl=%s', self.options.solrFields);
        url += util.format('&sort=%s', self.options.solrSort);
        url += util.format('&cursorMark=%s', encodeURIComponent(self.options.cursorMark));
        url += '&wt=json&indent=true';

        console.log(url);
        http.get(url, function (res) {
            var data = '';

            res.on('data', function (chunk) {
                data += chunk;
            });

            res.on('end', function () {
                if (parseInt(res.statusCode, 10) === 200) {
                    try {
                        var raw = JSON.parse(data);
                        var docs = raw.response.docs;

                        // Get the next token for the pagination
                        self.options.cursorMark = raw.nextCursorMark ? raw.nextCursorMark : '';
                        if (!self.options.cursorMark) {
                            throw new Error('It wasn\'t possible to get the next cursor mark for solr pagination. This should not happen!');
                        }

                        // check if there are posts to process
                        if (docs.length > 0) {
                            processListOfDocuments(docs, callback);
                        } else {
                            callback(null, 'Process finished!');
                        }
                        return;
                    } catch (e) {
                        // do nothing. Let's retry
                        console.log(e);
                    }
                }
                if (retry < 5) {
                    setTimeout(function retryGetDocumentsFromSolrTimeoutOnEnd() {
                        getDocumentsFromSolr(callback, ++retry);
                    }, 250);
                } else {
                    return callback('Error: It\'s not possible to get the documents from Solr or maybe the Solr database is down.');
                }
            });

        }).on('error', function (e) {
          if (retry < 5) {
            setTimeout(function retryGetDocumentsFromSolrTimeoutOnError() {
              getDocumentsFromSolr(callback, ++retry);
            }, 250);
          } else {
            return callback('Error: ' + e);
          }
        });
    };

    var processListOfDocuments = function (docs, callback) {
        // just process one document at a time
        processDocument(docs, 0, callback);
    };

    var processDocument = function (docs, idx, callback) {
        if (self.options.current + idx + 1 > self.options.MAX_DOCS) {
          // We need to stop the process here
          return callback(null, 'Process finished (MAX_DOCS reached)!');
        }

        if (idx < self.options.NUM_ROWS && idx < docs.length) {
            util.puts('\n\t====> '.blue.bold + (self.options.current + idx + 1).toString().yellow.bold + ' of '.blue.bold + (self.options.current + self.options.NUM_ROWS).toString().green.bold + ' - '.blue.bold + 'MAX_DOCS = '.red.bold + self.options.MAX_DOCS.toString().red.bold);

            self.options.onData(docs[idx]);

            setTimeout(function processNextDocument() {
                processDocument(docs, ++idx, callback);
            }, self.options.waitTime);
        } else {
            // let's process the next block
            self.options.current += self.options.NUM_ROWS;

            console.log('\nWaiting...');
            setTimeout(function setGetDocumentsFromSolrTimeout() {
                getDocumentsFromSolr(callback);
            }, self.options.waitTimeBlock);
        }
    };

    getDocumentsFromSolr(function (error, res) {
        if (error) {
            self.options.onError(error);
        } else {
            self.options.onFinish(res);
        }
    });
}