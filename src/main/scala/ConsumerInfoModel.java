import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class ConsumerInfoModel {
    @ApiModelProperty(notes = "Name of the topic",required=true, example = "topic_name")
    @NotEmpty
    @NotBlank
    @NotNull
    private String topicName;

    @ApiModelProperty(notes = "Name of the consumer",required=true, example = "consumer_name")
    @NotEmpty
    @NotBlank
    @NotNull
    private String consumerName;

    @ApiModelProperty(notes = "Start time",required=true, example = "1269576560000")
    @NotEmpty
    @NotBlank
    @NotNull
    private String startTime;

    @ApiModelProperty(notes = "End time",required=true, example = "1969576560000")
    @NotEmpty
    @NotBlank
    @NotNull
    private String endTime;

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }


    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }
}
