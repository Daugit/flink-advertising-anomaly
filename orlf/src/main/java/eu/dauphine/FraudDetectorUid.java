package eu.dauphine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class FraudDetectorUid extends KeyedProcessFunction<String, Event, AlertUid> {

    private static final long serialVersionUID = 1L;

    /**
     * Count the number of clicks per uid every quarter
     */
    private transient ListState<Long> clickStateQuarter;
    /**
     * Count the number of clicks per uid every hour
     */
    private transient ListState<Long> clickStateHour;
    /**
     * Count the number of displays per uid every hour
     */
    private transient ListState<Long> displayStateHour;

    private List<String> uids_to_remove;
    private Map<String,Float> map_uid_ctr;

    private long timestamp_start = (long) -1.0;
    private long timestamp_end = (long) -1.0;

    private int counter = 0;
    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Long> clickDescriptorQuarter = new ListStateDescriptor<>(
                "click",
                Types.LONG);
        clickStateQuarter = getRuntimeContext().getListState(clickDescriptorQuarter);

        ListStateDescriptor<Long> clickDescriptorHour = new ListStateDescriptor<>(
                "click",
                Types.LONG);
        clickStateHour = getRuntimeContext().getListState(clickDescriptorHour);

        ListStateDescriptor<Long> displayDescriptorHour = new ListStateDescriptor<>(
                "display",
                Types.LONG);
        displayStateHour = getRuntimeContext().getListState(displayDescriptorHour);
    }

    //retirer les uids avec plus de 20 clicks pour 15'
    @Override
    public void processElement(Event event,Context context,Collector<AlertUid> collector) throws Exception {
        if(timestamp_start <= 0.0) {
            timestamp_start = event.getTimestamp();
            uids_to_remove = new ArrayList<>();
            map_uid_ctr = new HashMap<>();
        }
        if(timestamp_end <= event.getTimestamp()) {
            timestamp_end = event.getTimestamp();
        }

        if(timestamp_end-timestamp_start >= 15*60) {

            System.out.println("++++++++++++++++++++++++++ BEFORE : "  +map_uid_ctr.keySet().size());

            Map<String, Float> result = map_uid_ctr.entrySet()
                    .stream()
                    .filter(map -> !uids_to_remove.contains(map.getKey()))
                    .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));

            double sum = 0.0;

            for (String key : result.keySet() ){
                sum+=result.get(key);
            }
            sum /= result.size();
            System.out.println("####################### " + sum + " #######################" + result.keySet().size());


            double sum2 = 0.0;

            for (String key : map_uid_ctr.keySet() ){
                sum2+=map_uid_ctr.get(key);
            }
            sum2 /= map_uid_ctr.size();
            System.out.println("++++++++++++++++++++++++++ No FILTER : " + sum2 + "+++++++++++++++++++++++++++++++++++"+map_uid_ctr.keySet().size());

            timestamp_start = event.getTimestamp();
            timestamp_end = event.getTimestamp();

            map_uid_ctr = new HashMap<>();
        }

        if(event.getEventType().equals("display")) {
            displayStateHour.add(event.getTimestamp());

            while(true) {
                Long _maxHour = (Long) Collections.max((List) displayStateHour.get());
                Long _minHour = (Long) Collections.min((List) displayStateHour.get());

                if (_maxHour - _minHour > 60 * 60) {
                    displayStateHour.update(new ArrayList<>());
                } else {
                    break;
                }
            }

        }
        if(event.getEventType().equals("click")){
            clickStateQuarter.add(event.getTimestamp());
            clickStateHour.add(event.getTimestamp());

            while(true) {
                Long _maxQuarter = (Long) Collections.max((List) clickStateQuarter.get());
                Long _minQuarter = (Long) Collections.min((List) clickStateQuarter.get());

                if (_maxQuarter - _minQuarter > 15 * 60) {
                    List<Long> tmp = ((List<Long>) clickStateQuarter.get());
                    tmp.remove(_minQuarter);

                    clickStateQuarter.update(tmp);
                } else {
                    int count = ((List<Long>) clickStateQuarter.get()).size();
                    if (count > 5) {
                        if(!uids_to_remove.contains(event.getUid())){
                            AlertUid alert = new AlertUid();
                            alert.setUid(event.getUid());
                            collector.collect(alert);
                            uids_to_remove.add(event.getUid());
                        }

                    }
                    break;
                }
            }
        }
        int tmp1 = ((List) clickStateHour.get()).size();
        float tmp2 = Math.max((float)((List) displayStateHour.get()).size(),(float)1.0);
        Float ctr = tmp1/tmp2;
        map_uid_ctr.put(event.getUid(),ctr);
    }
}