package com.flower.springbatch.batchjob;

import com.flower.springbatch.common.TestUserDTO;
import org.springframework.batch.support.annotation.Classifier;
import org.springframework.stereotype.Component;

@Component
public class UserExportFileRouterClassifier {

    @Classifier
    public String classify(TestUserDTO classifiable) {
        String userCountry = classifiable.getCountry().toLowerCase();
        if("usa".equals(userCountry)){
            return "usa";
        } else if ("china".equals(userCountry)){
            return "china";
        } else {
            return "others";
        }
    }
}
