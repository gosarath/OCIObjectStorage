import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

public class oStorePOC {

    public static void main(String[] args) throws Exception{


        //default OCI config file "~/.oci/config"
        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse("~/.oci/config");

        final ConfigFileAuthenticationDetailsProvider provider =
                new ConfigFileAuthenticationDetailsProvider(configFile);

        ObjectStorageClient objStoreClient = ObjectStorageClient.builder()
                .region(Region.US_SANJOSE_1)
                .build(provider);

        GetObjectRequest gor = GetObjectRequest.builder().namespaceName("ax8ur1kwn7yy").bucketName("InputData").objectName("SLead_50k.csv").build();
        GetObjectResponse getResponse = objStoreClient.getObject(gor);
        System.out.println("GetResponse :: " + getResponse);
        System.out.println("GetResponse code :: " + getResponse.get__httpStatusCode__());
        InputStream iStream = getResponse.getInputStream();

        /*
        BufferedReader reader = new BufferedReader(new InputStreamReader(iStream));
        while(reader.ready()) {
            String line = reader.readLine();
            System.out.println("Input file string : " + line);
        }
        */

        // Upload plain
        File initialFile = new File("./Data/Lead_50k.csv");
        InputStream inputStream = new FileInputStream(initialFile);
        long startTime = System.nanoTime();
        PutObjectRequest por = PutObjectRequest.builder()
                .namespaceName("ax8ur1kwn7yy")
                .bucketName("InputData")
                .objectName("SLead_50k_put.csv")
                .putObjectBody(inputStream)
                .build();
        PutObjectResponse putResponse = objStoreClient.putObject(por);
        System.out.println("PutResponse :: " + putResponse);
        System.out.println("PutResponse code :: " + putResponse.get__httpStatusCode__());
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println("Put Time : " + duration/1000000);

        // Delete plain
        DeleteObjectRequest dor = DeleteObjectRequest.builder()
                .namespaceName("ax8ur1kwn7yy")
                .bucketName("InputData")
                .objectName("SLead_50k_put.csv")
                .build();
        DeleteObjectResponse deleteResponse = objStoreClient.deleteObject(dor);
        System.out.println("DeleteResponse :: " + deleteResponse);
        System.out.println("DeleteResponse code :: " + deleteResponse.get__httpStatusCode__());

        // Upload MultiPart
        CreateMultipartUploadDetails cMUD = CreateMultipartUploadDetails.builder()
                .object("SLead_50k_mp.csv")
                .build();
        CreateMultipartUploadRequest cMUR = CreateMultipartUploadRequest.builder()
                .namespaceName("ax8ur1kwn7yy")
                .bucketName("InputData")
                .body$(cMUD)
                .build();
        CreateMultipartUploadResponse cMUResponse = objStoreClient.createMultipartUpload(cMUR);
        System.out.println("PutResponse :: " + cMUResponse);
        System.out.println("PutResponse code :: " + cMUResponse.get__httpStatusCode__());

        // MultiPart Import Approach
        Map<String, String> metadata = null;
        String contentType = null;
        String contentEncoding = null;
        String contentLanguage = null;
        File body = new File("./Data/Lead_50k.csv");
        // configure upload settings as desired
        UploadConfiguration uploadConfiguration =
                UploadConfiguration.builder()
                        .allowMultipartUploads(true)
                        .allowParallelUploads(true)
                        .build();

        UploadManager uploadManager = new UploadManager(objStoreClient, uploadConfiguration);

        PutObjectRequest request =
                PutObjectRequest.builder()
                        .namespaceName("ax8ur1kwn7yy")
                        .bucketName("InputData")
                        .objectName("Lead_50k_MP_put.csv")
                        .contentType(contentType)
                        .contentLanguage(contentLanguage)
                        .contentEncoding(contentEncoding)
                        .opcMeta(metadata)
                        .build();

        UploadManager.UploadRequest uploadDetails =
                UploadManager.UploadRequest.builder(body).allowOverwrite(true).build(request);

        // upload request and print result
        // if multi-part is used, and any part fails, the entire upload fails and will throw BmcException
        startTime = System.nanoTime();
        UploadManager.UploadResponse mp_put_response = uploadManager.upload(uploadDetails);
        System.out.println("MultiPart - PutResponse :: " + mp_put_response);
        System.out.println("MultiPart - ContentMd5 :: " + mp_put_response.getContentMd5());
        System.out.println("MultiPart - ETag :: " + mp_put_response.getETag());
        System.out.println("MultiPart - MultipartMd5 :: " + mp_put_response.getMultipartMd5());
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("Put Time : " + duration/1000000);

        // fetch the object just uploaded
        GetObjectResponse getResponseMP =
                objStoreClient.getObject(
                        GetObjectRequest.builder()
                                .namespaceName("ax8ur1kwn7yy")
                                .bucketName("InputData")
                                .objectName("Lead_50k_MP_put.csv")
                                .build());
        System.out.println("MultiPart - getResponse :: " + getResponseMP);
        System.out.println("MultiPart - getResponse Code :: " + getResponseMP.get__httpStatusCode__());

        DeleteObjectRequest mp_dor = DeleteObjectRequest.builder()
                .namespaceName("ax8ur1kwn7yy")
                .bucketName("InputData")
                .objectName("Lead_50k_MP_put.csv")
                .build();
        DeleteObjectResponse mp_deleteResponse = objStoreClient.deleteObject(mp_dor);
    }
}
