package com.package1.controller;

import com.package1.model.EventData;
import com.package1.model.SubscriberModel;
import com.package1.model.PublisherModel;
import com.package1.service.BrokerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import org.springframework.core.ParameterizedTypeReference;

@RestController
public class BrokerController {

    private HashMap<String, String> publisherStatusMap = new HashMap<>();

    // edited by @Manjula Mynampati
    RestTemplate restTemplate = new RestTemplate();
    private boolean isCurrentLeadBroker = false;
    private HashMap<String, List<SubscriberModel>> publisherSubscriberMap = new HashMap<>();
    private HashMap<String, EventData> publisherEventDataMap = new HashMap<>();
    //added @Manjula Mynampati
    private final ReentrantLock lock = new ReentrantLock();

    @Value("${configIp}")
    private String configIp;

    @Value("${ec2Port}")
    private String ec2Port;

    private String configUrl = "http://" + configIp + ":" + ec2Port;

    @Autowired
    private BrokerService brokerService;
    // ended by @Manjula Mynampati


    // This method is coming from subscriber to fetch list of active publishers @Manjula Mynampati
    @GetMapping(value = "/getPublishers")
    public ResponseEntity<List<String>> getPublishers() {
        List<String> activePublishers = new ArrayList<>();


        for (Map.Entry<String, String> entry : publisherStatusMap.entrySet()) {
            String publisherId = entry.getKey();
            String status = entry.getValue();

            if ("active".equals(status)) {
                activePublishers.add(publisherId);
            }
        }

        System.out.print("\n sending active publishers to subscriber:::::::" +activePublishers);

        return ResponseEntity.ok(activePublishers);
    }

    // This method is coming from subscriber to subscribe list of subscribers @Manjula Mynampati
    @PostMapping(value = "/subscribe")
    public ResponseEntity<HttpStatus> subscribe(@RequestBody SubscriberModel subscriberModel) {

        int subscriberId = subscriberModel.getSubscriberId();
        List<String> selectedPublishers = subscriberModel.getPublishers();

        lock.lock();
        try {
            for (String publisherId : selectedPublishers) {
                if (!publisherSubscriberMap.containsKey(publisherId)) {
                    publisherSubscriberMap.put(publisherId, new ArrayList<>());
                }
                List<SubscriberModel> subscribers = publisherSubscriberMap.get(publisherId);
                subscribers.add(subscriberModel);
            }

            boolean success = sendMapToPeerBrokers(publisherSubscriberMap); //consistency & Replication

            if(success){
                System.out.println("\n  Subscriber with ID " + subscriberId + " subscribed to publishers: " + selectedPublishers);
                return ResponseEntity.ok(HttpStatus.OK);
            }else{
                System.out.println("\n  Subscriber with ID " + subscriberId + " could not be subscribed to publishers ");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }



        }catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }  finally {
            lock.unlock();
        }

    }

    //this method should be called inside receiveEventDataFromPublisherAndSend
    public boolean notifySubscriber (List<SubscriberModel> subscribers, EventData event) {

        boolean flag = brokerService.notify(subscribers,event);

        return flag;

    }



    // sending publisherSubscriberMap to peer brokers for subscribe @ManjulaMynampati
    private boolean sendMapToPeerBrokers(HashMap<String, List<SubscriberModel>> publisherSubscriberMap) throws Exception {

        boolean flag = false;
        List<String> peerBrokersIpList = getPeerIpsFromConfigServer();


        for (String peerBrokerIP : peerBrokersIpList) {
            String peerurl = "http://" + peerBrokerIP + ":" + Integer.parseInt(ec2Port) + "/receiveHashMapUpdate";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<HashMap> requestEntity = new HttpEntity<>(publisherSubscriberMap,headers);
            System.out.println("Sending publisherSubscriberMap to peerBroker at url " + peerurl);
            HttpStatusCode status;
            try {
                ResponseEntity<HttpStatus> responseEntity = restTemplate.exchange(peerurl,
                        HttpMethod.POST,requestEntity,HttpStatus.class);

                status = responseEntity.getStatusCode();
            } catch (Exception e) {
                System.err.println("Exception while sending publisherSubscriberMap to Peer Broker at " + peerurl + ": " + e.getMessage());
                throw e;
            }

            if (!status.equals(HttpStatus.OK)) {
                flag = false;
                break;
            }else{
                flag = true;
            }
        }

        return flag;
    }


    // getting peer brokers ip from config server method to be updated @ManjulaMynampati
    public List<String> getPeerIpsFromConfigServer() {
        List<String> peerBrokersIpList = new ArrayList<>();

        try {

            String appendedUrl = configUrl + "/get-peerBrokers-IPList";
            System.out.println("Appended URL in : " + appendedUrl);
            ParameterizedTypeReference<List<String>> responseType = new ParameterizedTypeReference<List<String>>() {};

            ResponseEntity<List<String>> responseEntity  = restTemplate.exchange(
                    appendedUrl, HttpMethod.GET, null, responseType);

            if (responseEntity.getStatusCode().is2xxSuccessful()) {

                peerBrokersIpList = responseEntity.getBody();
                for (String peerBrokerIP : peerBrokersIpList) {
                    System.out.println("Peer Broker IP: " + peerBrokerIP);
                }
            } else {
                System.err.println("Failed to fetch peer broker IP list. Status code: " + responseEntity.getStatusCodeValue());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return peerBrokersIpList;
    }



    // receiving method to peerbrokers that publisherSubscriberMap is updated @ManjulaMynampati
    @PostMapping("/receiveHashMapUpdate")
    public ResponseEntity<String> receiveHashMapUpdate(@RequestBody HashMap<String, List<SubscriberModel>> receivedMap) {

        try {

            this.publisherSubscriberMap.clear();
            this.publisherSubscriberMap.putAll(receivedMap);

            System.out.print("Received publisherSubscriberMap from lead broker. Updated successfully");

            return ResponseEntity.ok("");


        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error updating publisherSubscriberMap: " + e.getMessage());
        }
    }


    @PostMapping(value = "/unsubscribe")
    public ResponseEntity<HttpStatus> unsubscribe(@RequestBody SubscriberModel subscriberModel) {

        int subscriberId = subscriberModel.getSubscriberId();
        List<String> selectedPublishers = subscriberModel.getPublishers();

        lock.lock();
        try {
            for (String publisherId : selectedPublishers) {
                if (publisherSubscriberMap.containsKey(publisherId)) {
                    List<SubscriberModel> subscribers = publisherSubscriberMap.get(publisherId);
                    // Remove the subscriber with the specified ID
                    subscribers.removeIf(subscriber -> subscriber.getSubscriberId() == subscriberId);
                }
            }

            boolean success = sendMapToPeerBrokers(publisherSubscriberMap); // Consistency & Replication

            if (success) {
                System.out.println("Subscriber with ID " + subscriberId + " unsubscribed from publishers: " + selectedPublishers);
                return ResponseEntity.ok(HttpStatus.OK);
            } else {
                System.out.println("Subscriber with ID " + subscriberId + " could not be unsubscribed from publishers");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            lock.unlock();
        }
    }
    /*
    @Shreya Krishnamoorthy
    */

    // sending publisherStatusMap to peer brokers for subscribe @Shreya Krishnamoorthy
    private boolean sendStatusMapToPeerBrokers(HashMap<String, String> publisherStatusMap) throws Exception {

        boolean flag = false;
        List<String> peerBrokersIpList = getPeerIpsFromConfigServer();


        for (String peerBrokerIP : peerBrokersIpList) {
            String peerurl = "http://" + peerBrokerIP + ":" + ec2Port + "/receiveStatusMapUpdate";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<HashMap> requestEntity = new HttpEntity<>(publisherStatusMap,headers);
            System.out.println("Sending publisherStatusMap to peerBroker at url " + peerurl);
            HttpStatus status;
            try {
                ResponseEntity<HttpStatus> responseEntity = restTemplate.exchange(peerurl,
                        HttpMethod.POST,requestEntity,HttpStatus.class);

                status = responseEntity.getBody();
            } catch (Exception e) {
                System.err.println("Exception while sending publisherStatusMap to Peer Broker at " + peerurl + ": " + e.getMessage());
                throw e;
            }

            if (!status.equals(HttpStatus.OK)) {
                flag = false;
                break;
            }else{
                flag = true;
            }
        }

        return flag;
    }

    // receiving method to peerbrokers that publisherStatusMap is updated @Shreya Krishnamoorthy
    @PostMapping("/receiveStatusMapUpdate")
    public ResponseEntity<String> receiveStatusMapUpdate(@RequestBody HashMap<String, String> receivedMap) {

        try {

            this.publisherStatusMap.clear();
            this.publisherStatusMap.putAll(receivedMap);

            return ResponseEntity.ok("Received publisherStatusMap from lead broker. Updated successfully");

        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error updating publisherStatusMap: " + e.getMessage());
        }
    }

    // This method is coming from publisher to change its status @Shreya Krishnamoorthy
    @PostMapping(value = "/changePublisherStatus")
    public ResponseEntity<HttpStatus> changePublisherStatus(@RequestBody PublisherModel publisherModel) {

        String publisherId = publisherModel.getPublisherId();
        String status = publisherModel.getStatus();
        boolean success = false;

        lock.lock();
        try {
            if (publisherStatusMap.containsKey(publisherId)) {
                publisherStatusMap.put(publisherId, status);
                success = sendStatusMapToPeerBrokers(publisherStatusMap); //consistency & Replication
            }
            if(success){
                System.out.println("Publisher name: " + publisherId + "| Status changed to: " + status);
                return ResponseEntity.ok(HttpStatus.OK);
            }
            else {
                System.out.println("Publisher name:" + publisherId + " not found in the status map");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }  finally {
            lock.unlock();
        }
    }

    // This method is coming from publisher to add publisher to the broker system @Shreya Krishnamoorthy
    @PostMapping(value = "/addPublisher")
    public ResponseEntity<HttpStatus> addPublisher(@RequestBody PublisherModel publisherModel) {

        String publisherId = publisherModel.getPublisherId();
        String status = publisherModel.getStatus();
        boolean successForSubscriberMap = false;
        boolean successForStatusMap = false;

        lock.lock();
        try {
            if (!publisherSubscriberMap.containsKey(publisherId)) {
                publisherSubscriberMap.put(publisherId, new ArrayList<>());
            }
            successForSubscriberMap = sendMapToPeerBrokers(publisherSubscriberMap); //consistency & Replication
            publisherStatusMap.put(publisherId,status);
            successForStatusMap = sendStatusMapToPeerBrokers(publisherStatusMap); //consistency & Replication

            if(successForSubscriberMap && successForStatusMap){
                System.out.println("Publisher: "  + publisherId + "added to the broker system");
                return ResponseEntity.ok(HttpStatus.OK);
            }
            else {
                System.out.println("Publisher: " + publisherId + "could not be added to the broker system");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }

        }catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }  finally {
            lock.unlock();
        }

    }

    // This method is coming from publisher to push its event to the broker @Shreya Krishnamoorthy
    @PostMapping(value = "/pushEvent")
    public ResponseEntity<HttpStatus> pushEvent(@RequestBody EventData eventData){
        String publisherId=eventData.getPublisherId();
        int eventId=eventData.getEventId();
        lock.lock();
        try{
            publisherEventDataMap.put(publisherId,eventData);
            System.out.println("Event data with id: "+eventId+" received by the broker");
            return ResponseEntity.ok(HttpStatus.OK);
            }  catch (Exception e) {
                e.printStackTrace();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }  finally {
                lock.unlock();
            }
    }
}



