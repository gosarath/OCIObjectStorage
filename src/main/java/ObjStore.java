import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;

import java.io.*;

public class ObjStore {
    String ConfigLoc;
    String Region ;
    String nameSpace;
    String Bucket;
    String object;
    String filePath;
    ObjectStorage objStore;

    public ObjStore(String ConfigLoc, String Region, String nameSpace, String Bucket, String object,String filePath) throws IOException {
        this.Bucket = Bucket;
        this.ConfigLoc = ConfigLoc;
        this.Region = Region;
        this.nameSpace = nameSpace;
        this.object = object;
        this.filePath = filePath;

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse(ConfigLoc);

        final ConfigFileAuthenticationDetailsProvider provider =
                new ConfigFileAuthenticationDetailsProvider(configFile);

        objStore = ObjectStorageClient.builder()
                .region(com.oracle.bmc.Region.US_SANJOSE_1)
                .build(provider);

    }

    public void upload () throws FileNotFoundException {

        // Upload plain
        File initialFile = new File(this.filePath);
        InputStream inputStream = new FileInputStream(initialFile);
        long startTime = System.nanoTime();
        PutObjectRequest por = PutObjectRequest.builder()
                .namespaceName(this.nameSpace)
                .bucketName(this.Bucket)
                .objectName(this.object)
                .putObjectBody(inputStream)
                .build();

        UploadPartResponse asd = objStore.uploadPart();
        asd.getETag();
        PutObjectResponse putResponse = objStore.putObject(por);
        System.out.println("PutResponse :: " + putResponse);
        System.out.println("PutResponse code :: " + putResponse.get__httpStatusCode__());
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println("Put Time : " + duration/1000000);
    }

    public void delete () {

        DeleteObjectRequest dor = DeleteObjectRequest.builder()
                .namespaceName(this.nameSpace)
                .bucketName(this.Bucket)
                .objectName(this.object)
                .build();
        DeleteObjectResponse deleteResponse = objStore.deleteObject(dor);
        System.out.println("DeleteResponse :: " + deleteResponse);
        System.out.println("DeleteResponse code :: " + deleteResponse.get__httpStatusCode__());
    }

    public void read() throws IOException {
        GetObjectRequest gor = GetObjectRequest.builder()
                .namespaceName(this.nameSpace)
                .bucketName(this.Bucket)
                .objectName(this.object)
                .build();
        GetObjectResponse getResponse = objStore.getObject(gor);
        System.out.println("GetResponse :: " + getResponse);
        System.out.println("GetResponse code :: " + getResponse.get__httpStatusCode__());
        InputStream iStream = getResponse.getInputStream();

        BufferedReader reader = new BufferedReader(new InputStreamReader(iStream));
        while(reader.ready()) {
            String line = reader.readLine();
            System.out.println("Input file string : " + line);
        }

    }

}
