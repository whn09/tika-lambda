package com.dovetailsoftware.aws.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.net.URLDecoder;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class TikaLambdaHandler implements RequestHandler<S3Event, String> {

    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Received S3 Event: " + s3event.toJson());
        Tika tika = new Tika();

        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String bucket = record.getS3().getBucket().getName();

            // Object key may have spaces or unicode non-ASCII characters.
            String key = URLDecoder.decode(record.getS3().getObject().getKey().replace('+', ' '), "UTF-8");

            // Short-circuit ignore .extract files because they have already been extracted, this prevents an endless loop
            if (key.toLowerCase().endsWith(".extract")) {
              logger.log("Ignoring extract file " + key);
              return "Ignored";
            }

            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));

            try (InputStream objectData = s3Object.getObjectContent()) {
                logger.log("Extracting text with Tika");
                String extractedText = tika.parseToString(objectData);

                byte[] extractBytes = extractedText.getBytes(Charset.forName("UTF-8"));
                int extractLength = extractBytes.length;
                logger.log(extractLength + " bytes extracted by Tika");

                ObjectMetadata metaData = new ObjectMetadata();
                metaData.setContentLength(extractLength);

                logger.log("Saving extract file to S3");
                InputStream inputStream = new ByteArrayInputStream(extractBytes);
                s3Client.putObject(bucket, key + ".extract", inputStream, metaData);
            }
        } catch (IOException | TikaException e) {
            logger.log("Exception: " + e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
        return "Success";
    }
}
