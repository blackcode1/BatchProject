package cn.edu.BatchCal;


import StreamDataPacket.BaseClassDataType.JarInfo;
import cn.edu.thss.rcsdk.BAlgFromSA;
import cn.edu.thss.rcsdk.RealTimeAlg;
import cn.edu.thss.rcsdk.BatchAlg;
import cn.edu.thss.rcsdk.StreamAlg;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class GetUserAlg {
    public static BatchAlg getUserAlg(Map<String, BatchAlg> userAlg, Map<String, JarInfo> jarInfoMap, String jarID) throws Exception {
        //return new DCECService();
        if(userAlg.containsKey(jarID)){
            return userAlg.get(jarID);
        }
        else {
            BatchAlg realTimeAlg = null;
            String jarPath = jarInfoMap.get(jarID).jarPath;
            String jarClass = jarInfoMap.get(jarID).jarClass;
            String type = jarInfoMap.get(jarID).type;
            realTimeAlg = loadjar(jarPath, jarClass,type);
            userAlg.put(jarID, realTimeAlg);
            return realTimeAlg;
        }
    }

    public static BatchAlg loadjar(String jarPath, String jarClass, String type)throws Exception{
        ClassLoader cl;
        BatchAlg rti = null;
        jarPath = "file:///d:/3/fuelconsume.jar";
        cl = new URLClassLoader(
                new URL[]{new URL(jarPath)},
                Thread.currentThread().getContextClassLoader());
        Class<?> myclass = cl.loadClass(jarClass);
        if(!type.equals("stream")){
            rti = (BatchAlg) myclass.newInstance();
        }
        else{
            StreamAlg batchAlg = (StreamAlg) myclass.newInstance();
            rti = new BAlgFromSA(batchAlg);
        }
        return rti;
    }
}
