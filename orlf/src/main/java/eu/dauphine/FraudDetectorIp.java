package eu.dauphine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

public class FraudDetectorIp extends KeyedProcessFunction<String, Event, AlertIp> {

    private static final long serialVersionUID = 1L;

    //private static final double SMALL_AMOUNT = 1.00;
    //private static final double LARGE_AMOUNT = 500.00;
    //private static final long ONE_MINUTE = 60 * 1000;
    private transient ListState<Long> clickState;
    private transient ListState<Long> displayState;

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

    //retirer les ips avec plus de 20 clicks/displays sur 15'
    @Override
    public void processElement(
            Event event,
            Context context,
            Collector<AlertIp> collector) throws Exception {
        if(event.getEventType().equals("click") || event.getEventType().equals("display")){

            if (event.getEventType().equals("click")) {clickState.add(event.getTimestamp()); }
            else {displayState.add(event.getTimestamp());}

            while(true) {
                Long _max_click;
                Long _min_click;
                Long _max_display;
                Long _min_display;

                if(((List)clickState.get()).size()==0) {
                    _max_click = 0L;
                    _min_click = 0L;
                } else {
                    _max_click = (Long) Collections.max((List) clickState.get());
                    _min_click = (Long) Collections.min((List) clickState.get());
                }

                if (((List)displayState.get()).size()==0) {
                    _max_display = 0L;
                    _min_display = 0L;
                } else {
                    _max_display = (Long) Collections.max((List) displayState.get());
                    _min_display = (Long) Collections.min((List) displayState.get());
                }


                if (_max_click - _min_click > 15 * 60 ||
                        _max_display - _min_display > 15 * 60) {

                    if (_max_click - _min_click > 15 * 60) {
                        List<Long> tmp = ((List<Long>) clickState.get());
                        tmp.remove(_min_click);
                        clickState.update(tmp); }

                    else {
                        List<Long> tmp = ((List<Long>) displayState.get());
                        tmp.remove(_min_display);
                        displayState.update(tmp);}

                } else {
                    int count = ((List<Long>) clickState.get()).size() +
                            ((List<Long>) displayState.get()).size();
                    if (count > 20) {
                        AlertIp alert = new AlertIp();
                        alert.setIp(event.getIp());
                        collector.collect(alert);
                    }
                    break;
                }
            }
        }
    }
}