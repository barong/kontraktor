package org.nustaq.kontraktor.remoting.classloading;


import org.xeustechnologies.jcl.JarClassLoader;

/**
 * Created by ruedi on 20.08.16.
 */
public class RemoteClassLoader {

    public static void main(String[] args) {
        JarClassLoader jcl=new JarClassLoader();
        jcl.add("myjar.jar"); // Add some class source

        jcl.getSystemLoader().setOrder(1); // Look in system class loader first
        jcl.getLocalLoader().setOrder(2); // if not found look in local class loader
        jcl.getParentLoader().setOrder(3); // if not found look in parent class loader
        jcl.getThreadLoader().setOrder(4); // if not found look in thread context class loader
        jcl.getCurrentLoader().setOrder(5); // if not found look in current class loader

        // A custom class loader that extends org.xeustechnologies.jcl.ProxyClassLoader
//        MyLoader loader=new MyLoader();
//        loader.setOrder(6);

//        jcl.addLoader(loader); //Add custom loader
    }
}
