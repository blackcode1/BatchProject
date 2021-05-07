package BatchCal;

import StreamDataPacket.BaseClassDataType.JarInfo;
import cn.edu.thss.rcsdk.RealTimeAlg;
import thss.rcsdk.BatchAlg;

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
            realTimeAlg = loadjar(jarPath, jarClass);
            userAlg.put(jarID, realTimeAlg);
            return realTimeAlg;
        }
    }

    public static BatchAlg loadjar(String jarPath, String jarClass)throws Exception{
        ClassLoader cl;
        RealTimeAlg rti = null;
        cl = new URLClassLoader(
                new URL[]{new URL(jarPath)},
                Thread.currentThread().getContextClassLoader());
        Class<?> myclass = cl.loadClass(jarClass);
        rti = (RealTimeAlg) myclass.newInstance();
        return rti;
    }
}
