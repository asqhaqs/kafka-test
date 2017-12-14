/*
 * msg_encry.h
 *
 *  Created on: Dec 22, 2014
 *      Author: turing
 */

#ifndef READ_MEMORY_C_MSG_ENCRY_H_
#define READ_MEMORY_C_MSG_ENCRY_H_

#include "aes.h"

#define PRODUCT_SERIAL_LEN 9
#define ENCRYPT_HEADER_LEN 16

#define AES_OLD_ENCRYPT		    1
#define AES_NI_ENCRYPT 		    6
#define AES_OPENSSL_ENCRYPT 	5

typedef struct _aes_encrypt{
    struct crypto_aes_ctx ctx;
    //int encrypt_onoff; /*this zmq action is need to encrypt or not*/
    int encrypt_key_rd; /*this zmq action's key is ready*/
    int key[32]; /*this zmq action's key*/
}aes_encrypt_t;

//extern int init_aes(aes_encrypt_t *encry, int en_type);
extern int init_aes(char *file);
//1 不加密；2 老的加密方式； 5 aes_ni; 6 aes_openssl
extern int encrypt(char *data_buff, int data_len, char *encrypt_buff);
extern int decrypt(char *data_buff, int data_len, char *decrypt_buff);
#endif /* READ_MEMORY_C_MSG_ENCRY_H_ */
