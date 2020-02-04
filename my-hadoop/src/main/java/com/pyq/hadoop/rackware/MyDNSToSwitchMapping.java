package com.pyq.hadoop.rackware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 机架感知自定义类
 *
 * @author Yangqi.Pang
 * @date 2020-01-31 08:58
 */
public class MyDNSToSwitchMapping implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> names) {
        writeToLog(names);
        List<String> list = new ArrayList<>();
        names.forEach(name ->{
            if(name.startsWith("s")){
                int ip = Integer.parseInt(name.substring(1));
                if(ip <= 105){
                    list.add("/a/"+name);
                }else {
                    list.add("/b/"+name);
                }
            }else if(name.contains(".")) {
                String[] arr = name.split("\\.");
                int ip = Integer.parseInt(arr[arr.length - 1]);
                if(ip <= 105){
                    list.add("/a/"+name);
                }else {
                    list.add("/b/"+name);
                }
            }
        });
        return list;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> list) {

    }

    private void writeToLog(List<String> names){
        try {
            FileOutputStream fos = new FileOutputStream("/home/centos/rack.log",true);
            fos.write("==================".getBytes());
            fos.write("\r\n".getBytes());
            for (String name : names) {
                fos.write(name.getBytes());
                fos.write("\r\n".getBytes());
            }
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
