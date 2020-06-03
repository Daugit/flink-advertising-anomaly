package eu.dauphine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FraudDetectorUid extends KeyedProcessFunction<String, Event, AlertUid> {

    private static final long serialVersionUID = 1L;

    //private static final double SMALL_AMOUNT = 1.00;
    //private static final double LARGE_AMOUNT = 500.00;
    //private static final long ONE_MINUTE = 60 * 1000;
    private transient ListState<Long> clickState;
    private transient ListState<Long> displayState;

    private transient List<String> uids_to_remove = new ArrayList<>();

    private int counter = 0;
    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Long> clickDescriptor = new ListStateDescriptor<>(
                "click",
                Types.LONG);
        clickState = getRuntimeContext().getListState(clickDescriptor);

        ListStateDescriptor<Long> displayDescriptor = new ListStateDescriptor<>(
                "display",
                Types.LONG);
        displayState = getRuntimeContext().getListState(displayDescriptor);
    }

    //retirer les uids avec plus de 20 clicks pour 15'
    @Override
    public void processElement(Event event,Context context,Collector<AlertUid> collector) throws Exception {

        if(event.getEventType().equals("click")){
            clickState.add(event.getTimestamp());

            while(true) {
                Long _max = (Long) Collections.max((List) clickState.get());
                Long _min = (Long) Collections.min((List) clickState.get());

                if (_max - _min > 15 * 60) {
                    List<Long> tmp = ((List<Long>) clickState.get());
                    tmp.remove(_min);

                    clickState.update(tmp);
                } else {
                    int count = ((List<Long>) clickState.get()).size();
                    if (count > 5) {
                        System.out.println(event.getUid());
                        AlertUid alert = new AlertUid();
                        alert.setUid(event.getUid());

                        if(!uids_to_remove.contains(event.getUid())){
                            uids_to_remove.add(event.getUid());
                        }
                        collector.collect(alert);
                    }
                    break;
                }
            }
        }
    }
}