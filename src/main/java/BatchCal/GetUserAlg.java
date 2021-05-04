package BatchCal;

import StreamDataPacket.BaseClassDataType.JarInfo;
import cn.edu.thss.rcsdk.RealTimeAlg;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class GetUserAlg {
    public static RealTimeAlg getUserAlg(Map<String, RealTimeAlg> userAlg, Map<String, JarInfo> jarInfoMap, String jarID) throws Exception {
        //return new DCECService();
        if(userAlg.containsKey(jarID)){
            return userAlg.get(jarID);
        }
        else {
            RealTimeAlg realTimeAlg = null;
            String jarPath = jarInfoMap.get(jarID).jarPath;
            String jarClass = jarInfoMap.get(jarID).jarClass;
            realTimeAlg = loadjar(jarPath, jarClass);
            userAlg.put(jarID, realTimeAlg);
            return realTimeAlg;
        }
    }

    public static RealTimeAlg loadjar(String jarPath, String jarClass)throws Exception{
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
