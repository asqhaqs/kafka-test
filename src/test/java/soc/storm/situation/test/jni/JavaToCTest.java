
package soc.storm.situation.test.jni;

/**
 * http://blog.csdn.net/rocgege/article/details/58585225
 * http://blog.csdn.net/shaohuazuo/article/details/17098157
 * 
 * @author wangbin03
 *
 */
public class JavaToCTest {

    private native void sayHello();

    static {
        // System.loadLibrary("JavaToCTest");
        // System.load("/home/swan/test/libJavaToCTest.so");
    }

    public static void main(String[] args) {
        new JavaToCTest().sayHello();
    }
}

// JavaToCTest.h
// /* DO NOT EDIT THIS FILE - it is machine generated */
// #include <jni.h>
// /* Header for class JavaToCTest */
//
// #ifndef _Included_JavaToCTest
// #define _Included_JavaToCTest
// #ifdef __cplusplus
// extern "C" {
// #endif
// /*
// * Class: JavaToCTest
// * Method: sayHello
// * Signature: ()V
// */
// JNIEXPORT void JNICALL Java_JavaToCTest_sayHello
// (JNIEnv *, jobject);
//
// #ifdef __cplusplus
// }
// #endif
// #endif

//
// JavaToCTest.c
// #include <stdio.h>
// #include <jni.h>
// #include "JavaToCTest.h"
//
// JNIEXPORT void JNICALL Java_JavaToCTest_sayHello(JNIEnv *env ,jobject obj)
// {
// printf("Java To C !!!!!   \n");
// return;
// }
