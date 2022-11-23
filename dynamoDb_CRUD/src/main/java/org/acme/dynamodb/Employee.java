package org.acme.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import io.quarkus.runtime.annotations.RegisterForReflection;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@RegisterForReflection
@DynamoDBTable(tableName="Company")
public record Employee(
        String PK,
        String SK,
        String name,
        Integer salary,
        String function,
        String GSI1_PK,
        String GSI1_SK,
        String location,
        List<String> historyLocation,
        String GSI2_PK,
        String GSI2_SK){

    //este método é necessário para transformar o item retornado do dynamoDB em Employee
    public static Employee converter(Map<String, AttributeValue> item) {

        List<String> historyLocation = new ArrayList<>();
        for (int i = 0; i < item.get(AbstractService.HISTORY_LOCATION).l().size(); i++) {
            historyLocation.add(item.get(AbstractService.HISTORY_LOCATION).l().get(i).s());
        }

        if (item != null && !item.isEmpty()) {
            Employee employee = new Employee(
                    item.get(AbstractService.PK_COL).s(),
                    item.get(AbstractService.SK_COL).s(),
                    item.get(AbstractService.NAME_COL).s(),
                    Integer.parseInt(item.get(AbstractService.SALARY_COL).n()), //parseInt porque vem em String do dynamoDB ( o dynamo so trabalha com strings)
                    item.get(AbstractService.FUNCTION_COL).s(),
                    item.get(AbstractService.PK_COL).s(),//O GSI1-PK é sempre o pk
                    item.get(AbstractService.NAME_COL).s(),//O GSI1-SK é sempre o nome
                    item.get(AbstractService.LOCATION).s(),
                    historyLocation,
                    item.get(AbstractService.LOCATION).s(),//O GSI2-PK é sempre a location
                    item.get(AbstractService.SK_COL).s());//O GSI2-SK é sempre o SK
            return employee;
        }
        return null;
    }

}