package com.serverless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Dummy {

    public static void main(String[] args) throws IOException {
        String json = null;

        int stringLength = 200;
        char[] chars = new char[stringLength];
        Arrays.fill(chars, 'x');
        json = new String(chars);

       /* String file = "src/main/resources/myJsonFile.json";
        try {
            json = new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        Integer objectSize = getObjectSize(Collections.singletonList(json));
        if(objectSize > 262144){
            System.out.println("The size of the object is greater than 256 KB " + objectSize);
        }else{
            System.out.println("The size of the object is less than 256 KB " + objectSize);
        }

    }

    private static int getObjectSize(List object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        return baos.size();
    }
}
