package eu.dauphine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FraudDetectorCtr extends KeyedProcessFunction<String, Event, AlertCtr> {


    private static final long serialVersionUID = 1L;

    //private static final double SMALL_AMOUNT = 1.00;
    //private static final double LARGE_AMOUNT = 500.00;
    //private static final long ONE_MINUTE = 60 * 1000;
    private transient ListState<Long> clickState;
    private transient ListState<Long> displayState;
    private transient ListState<String> EventState;

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

    //Afficher le CTR sur 60 mins en enlevant les uids/ip en alerte
    @Override
    public void processElement(
            Event event,
            Context context,
            Collector<AlertCtr> collector) throws Exception {

        if (event.getEventType().equals("click") || event.getEventType().equals("display")) {
            EventState.add([event.getEventType(),
                            event.getTimestamp(),
                            event.getIp(),
                            event.getUid(),
                            event.getImpressionId()]);
        }

        Long _max = (Long) Collections.max((List) EventState.getTimestamp());
        Long _min = (Long) Collections.min((List) EventState.getTimestamp());
        if (_max - _min > 60 * 60 ) {
            List<String> tmp = ((List<Long>) EventState.get());
            tmp.remove(_min);
            clickState.update(tmp);
            if(_max - _min <= 60 * 60 ) {
                int CTR = ((List<Long>) EventState.get()).size() +
                    ((List<Long>) EventState.get()).size();
                System.out.println(CTR);


            }
        }
    }
}