 #include <stdio.h>
 #include <jni.h>
 #include "TestByteArrayJni.h"


/*
 * Class:     TestByteArrayJni
 * Method:    encrypt
 * Signature: ([BIILjava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_TestByteArrayJni_encrypt
  (JNIEnv *env, jobject obj, jbyteArray a, jint b, jint c, jstring d){
	return a;
}

/*
 * Class:     TestByteArrayJni
 * Method:    decrypt
 * Signature: ([BIILjava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_TestByteArrayJni_decrypt
  (JNIEnv *env, jobject obj, jbyteArray a, jint b, jint c, jstring d){
  	return a;
}

 