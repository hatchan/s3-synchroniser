#!/usr/bin/env node

var program = require("commander");

program
  .version("0.0.1")
  .option("--sourceAccessKeyId <accessKeyId>", "the accedKeyId for the source bucket (uses $AWS_ACCESS_KEY_ID if not supplied)")
  .option("--sourceSecretAccessKey <secretAccessKey>", "the secretAccessKey for the source bucket (uses $AWS_SECRET_ACCESS_KEY if not supplied)")
  .option("--sourceRegion <region>", "the region of the source bucket (uses 'us-east-1' if not supplied)", "us-east-1")
  .option("--destinationAccessKeyId <accessKeyId>", "the accedKeyId for the destination bucket (uses sourceAccessKeyId if not supplied)")
  .option("--destinationSecretAccessKey <secretAccessKey>", "the secretAccessKey for the destination bucket (uses sourceSecretAccessKey if not supplied)")
  .option("--destinationRegion <region>", "the region of the destination bucket (uses sourceRegion if not supplied)")
  .option("--prefix <prefix>", "the region of the source bucket (uses 'us-east-1' if not supplied)", "us-east-1")
  .command("sync <sourceBucket> <destinationBucket>")
    .action(function(sourceBucket, destinationBucket, command) {
      var s3Synchronize = require("../lib/s3-synchronize");
      var util = require("util");

      var options = {
        "sourceAccessKeyId": program.sourceAccessKeyId,
        "sourceSecretAccessKey": program.sourceSecretAccessKey,
        "sourceRegion": program.sourceRegion,
        "destinationAccessKeyId": program.destinationAccessKeyId,
        "destinationSecretAccessKey": program.destinationSecretAccessKey,
        "destinationRegion": program.destinationRegion,
        "concurrentUploads": parseInt(process.env.CONCURRENT_UPLOADS, 10),
        "prefix": program.prefix,
      };

      s3Synchronize.synchronizeS3Buckets(sourceBucket, destinationBucket, options, function(err) {
        if (err) {
          console.error("Unable to synchronize buckets");
          console.error(util.inspect(err));
        }
        else {
          console.log("Synchronization done");
        }
      });

    });

if (process.argv.length === 2) {
  console.log(program.help());
}
else {
  program.parse(process.argv);
}
