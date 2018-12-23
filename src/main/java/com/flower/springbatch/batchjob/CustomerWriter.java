package com.flower.springbatch.batchjob;

import com.flower.springbatch.common.TestUserDTO;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

public class CustomerWriter implements ItemWriter<TestUserDTO>, ItemStream {

    private List<TestUserDTO> result = new ArrayList<TestUserDTO>();
    private int currentLocation = 0;
    private static final String CURRENT_LOCATION = "current.location";


    @Override
    public void open(ExecutionContext executionContext)
            throws ItemStreamException {
        if(executionContext.containsKey(CURRENT_LOCATION)){
            currentLocation = new Long(executionContext.getLong(CURRENT_LOCATION)).intValue();
        }else{
            currentLocation = 0;
        }
    }

    @Override
    public void update(ExecutionContext executionContext)
            throws ItemStreamException {
        executionContext.putLong(CURRENT_LOCATION, new Long(currentLocation).longValue());
    }

    @Override
    public void close() throws ItemStreamException {}

    public List<TestUserDTO> getResult() {
        return result;
    }

    @Override
    public void write(List<? extends TestUserDTO> items) throws Exception {

        System.out.println(items.size()+"===========================");
        for(;currentLocation < items.size();){
            result.add(items.get(currentLocation++));
        }
        System.out.println(result.size()+"===========================");
    }
}
