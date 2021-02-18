package spark;


import java.io.File;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3Uploader{

    private String bucketName;
    private String prefix;
    private String credentials_profile_name;

    public S3Uploader(String _bucketName, String _prefix, String _credentials_profile_name) {

        this.bucketName = _bucketName;
        this.prefix = _prefix;
        this.credentials_profile_name = _credentials_profile_name;
    }

    public void upload(String file) {

        /*
         * Creating client connection
         */
        String filename = file;
        String filekey = this.prefix + "-" + filename;

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();

        if (!s3Client.doesBucketExistV2(this.bucketName)) {
            s3Client.createBucket(this.bucketName);
        }
        try {
            PutObjectRequest request = new PutObjectRequest(this.bucketName, filekey, new File(filename));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
        }

    }

}

