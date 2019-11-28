package dojo.localstack.org;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class EjemploSQS {
    public static void main(String[] args) {
        /*
         * Crear una instancia de SQS
         */
    	
    	BasicAWSCredentials credenciales = new BasicAWSCredentials("access_key_id", "secret_key_id");
    	
        final AmazonSQS sqs = AmazonSQSClientBuilder.standard()
        		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:4576", "us-east-1"))
        		.withCredentials(new AWSStaticCredentialsProvider(credenciales)).build();
        System.out.println("===========================================");
        System.out.println("     Consumo de SQS en LocalStack");
        System.out.println("===========================================\n");
        
        try {

            // Crear una cola FIFO
        	
            System.out.println("Creando una cola llamada " +
                    "ColaFifo.fifo.\n");
            final Map<String, String> attributes = new HashMap<String, String>();

            // Una cola FIFO debe tener el atributo FifoQueue seteado en true.
            attributes.put("FifoQueue", "true");
       
            attributes.put("ContentBasedDeduplication", "true");

            // Los nombres de las colas FIFO deben poseer el sufijo .fifo
            
            final CreateQueueRequest createQueueRequest =
                    new CreateQueueRequest("ColaFifo.fifo")
                            .withAttributes(attributes);
            final String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            System.out.print(myQueueUrl + "\n");
            
            // Listar todas las colas
            System.out.println("Listando todas las colas en la cuenta\n");
            for (final String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();

            // Enviar un mensaje a la cola.
            System.out.println("Enviando un mensaje a ColaFifo.fifo.\n");
            final SendMessageRequest sendMessageRequest =
                    new SendMessageRequest(myQueueUrl,
                            "Este es un mensaje enviado a la cola");
            sendMessageRequest.setMessageGroupId("easyHome");

            final SendMessageResult sendMessageResult = sqs
                    .sendMessage(sendMessageRequest);
            final String sequenceNumber = sendMessageResult.getSequenceNumber();
            final String messageId = sendMessageResult.getMessageId();
            System.out.println("Mensaje enviado satisfactoriamente con messageId "
                    + messageId + ", numero de secuencia " + sequenceNumber + "\n");

            // Recibir mensajes de la cola.
            System.out.println("Recibiendo mensajes desde ColaFifo.fifo.\n");
            final ReceiveMessageRequest receiveMessageRequest =
                    new ReceiveMessageRequest(myQueueUrl);

            final List<Message> messages = sqs.receiveMessage(receiveMessageRequest)
                    .getMessages();
            for (final Message message : messages) {
                System.out.println("Mensaje");
                System.out.println("  MessageId:     "
                        + message.getMessageId());
                System.out.println("  ReceiptHandle: "
                        + message.getReceiptHandle());
                System.out.println("  MD5OfBody:     "
                        + message.getMD5OfBody());
                System.out.println("  Body:          "
                        + message.getBody());
                for (final Entry<String, String> entry : message.getAttributes()
                        .entrySet()) {
                    System.out.println("Attribute");
                    System.out.println("  Name:  " + entry.getKey());
                    System.out.println("  Value: " + entry.getValue());
                }
            }
        } catch (final AmazonServiceException ase) {
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (final AmazonClientException ace) {
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}