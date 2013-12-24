var async = require("async");
var AWS = require("aws-sdk");
var underscore = require("underscore");

var S3_API_VERION = "2006-03-01";

var _createS3 = function(accessKeyId, secretAccessKey, region) {
    var options = {
        "accessKeyId": accessKeyId,
        "secretAccessKey": secretAccessKey,
        "region": region,
        "apiVersion": S3_API_VERION,
    };

    var s3 = new AWS.S3(options);

    return s3;
};

var _listBucket = function(s3, bucket, prefix, items, callback) {
    var params = {
        "Bucket": bucket,
        "Prefix": prefix,
    };

    if (items.length > 0) {
        var lastKey = underscore.last(items).Key;
        params.Marker = lastKey;
    }

    s3.listObjects(params, function(err, data) {
        if (err) {
            console.error("Unable to list objects for bucket: " + bucket);
            callback(err);
        }
        else {
            items = items.concat(data.Contents);
            if (data.IsTruncated) {
                _listBucket(s3, bucket, prefix, items, callback);
            }
            else {
                var contents = underscore.indexBy(items, "Key");
                callback(null, contents);
            }
        }
    });
};

exports.listBucket = function(s3, bucket, prefix, callback) {
    _listBucket(s3, bucket, prefix, [], callback);
};

exports.synchronizeS3Buckets = function(sourceBucket, destinationBucket, options, callback) {

    var sourceAccessKeyId = options.sourceAccessKeyId || process.env.AWS_ACCESS_KEY_ID;
    var sourceSecretAccessKey = options.sourceSecretAccessKey || process.env.AWS_SECRET_ACCESS_KEY;
    var sourceRegion = options.sourceRegion || "us-east-1";
    var sourcePrefix = options.sourcePrefix || "";

    var destinationAccessKeyId = options.destinationAccessKeyId || sourceAccessKeyId;
    var destinationSecretAccessKey = options.destinationSecretAccessKey || sourceSecretAccessKey;
    var destinationRegion = options.destinationRegion || sourceRegion;
    var destinationPrefix = options.sourcePrefix || sourcePrefix;

    // TODO: validate

    var sourceS3 = _createS3(sourceAccessKeyId, sourceSecretAccessKey, sourceRegion);
    var destinationS3 = _createS3(destinationAccessKeyId, destinationSecretAccessKey, destinationRegion);

    var listSourceItems = function(next) {
        exports.listBucket(sourceS3, sourceBucket, sourcePrefix, next);
    };

    var listDestinationItems = function(next) {
        exports.listBucket(destinationS3, destinationBucket, destinationPrefix, next);
    };

    var uploadMissingItems = function(next, results) {
        var files = underscore.
        _uploadFiles(sourceS3, destinationS3, files, next);
    };

    async.auto({
        "sourceItems": listSourceItems,
        "destinationItems": listDestinationItems,
        "uploadMissingItems": ["sourceItems", "destinationItems", uploadMissingItems],
        // "deleteExtraItems": ["uploadMissingItems", deleteExtraItems],
        // "syncChangedItems": ["deleteExtraItems", overwriteChangedItems],
    }, callback);

    // Create source S3 object
    // Create destination S3 object
    // List keys in sourceBucket
    // List keys in destinationBucket
    // Compare keys
    // Download and upload missing keys
    // Overwrite non matching keys
    // Remove extra keys
};