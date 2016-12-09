/*
 * Copyright 2012-2016 Rufus Pollock.
 *
 * Licensed under the MIT license:
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * https://github.com/rgrp/s3-bucket-listing
 */
if (typeof S3BL_IGNORE_PATH == 'undefined' || S3BL_IGNORE_PATH!=true) {
  var S3BL_IGNORE_PATH = false;
}

if (typeof BUCKET_URL == 'undefined') {
  var BUCKET_URL = location.protocol + '//' + location.hostname;
}

if (typeof BUCKET_NAME != 'undefined') {
    // if bucket_url does not start with bucket_name,
    // assume path-style url
    if (!~BUCKET_URL.indexOf(location.protocol + '//' + BUCKET_NAME)) {
        BUCKET_URL += '/' + BUCKET_NAME;
    }
}

if (typeof BUCKET_WEBSITE_URL == 'undefined') {
  var BUCKET_WEBSITE_URL = BUCKET_URL;
}

if (typeof S3B_ROOT_DIR == 'undefined') {
  var S3B_ROOT_DIR = '';
}

jQuery(function($) {
  getS3Data();
});

function getS3Data(marker, html) {
  var s3_rest_url = createS3QueryUrl(marker);
  // set loading notice
  $('#listing').html('<img src="/assets/ajaxload-circle.gif" />');
  $.get(s3_rest_url)
    .done(function(data) {
      // clear loading notice
      $('#listing').html('');
      var xml = $(data);
      var info = getInfoFromS3Data(xml);

      buildNavigation(info)

      html = typeof html !== 'undefined' ? html + prepareTable(info) : prepareTable(info);
      if (info.nextMarker != "null") {
        getS3Data(info.nextMarker, html);
      } else {
        document.getElementById('listing').innerHTML = '<pre>' + html + '</pre>';
      }
    })
    .fail(function(error) {
      console.error(error);
      $('#listing').html('<strong>Error: ' + error + '</strong>');
    });
}

function buildNavigation(info) {
  var root = '<a href="?prefix=">' + BUCKET_WEBSITE_URL + '</a> / '
  if (info.prefix) {
    var processedPathSegments = ''
    var content = $.map(info.prefix.split('/'), function(pathSegment){
      processedPathSegments = processedPathSegments + encodeURIComponent(pathSegment) + '/'
      return '<a href="?prefix=' + processedPathSegments + '">' + pathSegment + '</a>'
    });
    $('#navigation').html(root + content.join(' / '))
  } else {
    $('#navigation').html(root)
  }
}

function createS3QueryUrl(marker) {
  var s3_rest_url = BUCKET_URL;
  s3_rest_url += '?delimiter=/';

  //
  // Handling paths and prefixes:
  //
  // 1. S3BL_IGNORE_PATH = false
  // Uses the pathname
  // {bucket}/{path} => prefix = {path}
  //
  // 2. S3BL_IGNORE_PATH = true
  // Uses ?prefix={prefix}
  //
  // Why both? Because we want classic directory style listing in normal
  // buckets but also allow deploying to non-buckets
  //

  var rx = '.*[?&]prefix=' + S3B_ROOT_DIR + '([^&]+)(&.*)?$';
  var prefix = '';
  if (S3BL_IGNORE_PATH==false) {
    var prefix = location.pathname.replace(/^\//, S3B_ROOT_DIR);
  }
  var match = location.search.match(rx);
  if (match) {
    prefix = S3B_ROOT_DIR + match[1];
  } else {
    if (S3BL_IGNORE_PATH) {
      var prefix = S3B_ROOT_DIR;
    }
  }
  if (prefix) {
    // make sure we end in /
    var prefix = prefix.replace(/\/$/, '') + '/';
    s3_rest_url += '&prefix=' + prefix;
  }
  if (marker) {
    s3_rest_url += '&marker=' + marker;
  }
  return s3_rest_url;
}

function getInfoFromS3Data(xml) {
  var files = $.map(xml.find('Contents'), function(item) {
    item = $(item);
    return {
      Key: item.find('Key').text(),
      LastModified: item.find('LastModified').text(),
      Size: bytesToHumanReadable(item.find('Size').text()),
      Type: 'file'
    }
  });
  var directories = $.map(xml.find('CommonPrefixes'), function(item) {
    item = $(item);
    return {
      Key: item.find('Prefix').text(),
      LastModified: '',
      Size: '0',
      Type: 'directory'
    }
  });
  if ($(xml.find('IsTruncated')[0]).text() == 'true') {
    var nextMarker = $(xml.find('NextMarker')[0]).text();
  } else {
    var nextMarker = null;
  }
  return {
    files: files,
    directories: directories,
    prefix: $(xml.find('Prefix')[0]).text(),
    nextMarker: encodeURIComponent(nextMarker)
  }
}

// info is object like:
// {
//    files: ..
//    directories: ..
//    prefix: ...
// }
function prepareTable(info) {
  var files = info.files.concat(info.directories)
    , prefix = info.prefix
    ;
  var cols = [ 45, 30, 15 ];
  var content = [];
  content.push(padRight('Last Modified', cols[1]) + '  ' + padRight('Size', cols[2]) + 'Key \n');
  content.push(new Array(cols[0] + cols[1] + cols[2] + 4).join('-') + '\n');

  // add the ../ at the start of the directory listing, unless when at the root dir already
  if (prefix && prefix !== S3B_ROOT_DIR) {
    var up = prefix.replace(/\/$/, '').split('/').slice(0, -1).concat('').join('/'), // one directory up
      item = {
        Key: up,
        LastModified: '',
        Size: '',
        keyText: '../',
        href: S3BL_IGNORE_PATH ? '?prefix=' + up : '../'
      },
      row = renderRow(item, cols);
    content.push(row + '\n');
  }

  jQuery.each(files, function(idx, item) {
    // strip off the prefix
    item.keyText = item.Key.substring(prefix.length);
    if (item.Type === 'directory') {
      if (S3BL_IGNORE_PATH) {
        item.href = location.protocol + '//' + location.hostname + location.pathname + '?prefix=' + item.Key;
      } else {
        item.href = item.keyText;
      }
    } else {
      item.href = BUCKET_WEBSITE_URL + '/' + encodeURIComponent(item.Key);
      item.href = item.href.replace(/%2F/g, '/');
    }
    var row = renderRow(item, cols);
    content.push(row + '\n');
  });

  return content.join('');
}

function renderRow(item, cols) {
  var row = '';
  row += padRight(item.LastModified, cols[1]) + '  ';
  row += padRight(item.Size, cols[2]);
  row += '<a href="' + item.href + '">' + item.keyText + '</a>';
  return row;
}

function padRight(padString, length) {
  var str = padString.slice(0, length-3);
  if (padString.length > str.length) {
    str += '...';
  }
  while (str.length < length) {
    str = str + ' ';
  }
  return str;
}

function bytesToHumanReadable(sizeInBytes) {
  var i = -1;
  var units = [' kB', ' MB', ' GB'];
  do {
    sizeInBytes = sizeInBytes / 1024;
    i++;
  } while (sizeInBytes > 1024);
  return Math.max(sizeInBytes, 0.1).toFixed(1) + units[i];
}
