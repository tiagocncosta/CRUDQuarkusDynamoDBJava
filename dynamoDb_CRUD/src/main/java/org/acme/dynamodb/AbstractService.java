package org.acme.dynamodb;

import java.util.*;
import java.util.function.Consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import javax.inject.Inject;
import javax.swing.plaf.synth.Region;

public abstract class AbstractService {


    public static final String PK_COL = "PK";
    public static final String SK_COL = "SK";
    public static final String NAME_COL = "Name";
    public static final String SALARY_COL = "Salary";
    public static final String FUNCTION_COL = "Function";
    public static final String GSI1_PK = "GSI1-PK";
    public static final String GSI1_SK = "GSI1-SK";
    public static final String LOCATION = "Location";
    public static final String HISTORY_LOCATION = "HistoryLocation";
    public static final String GSI2_PK = "GSI2-PK";
    public static final String GSI2_SK = "GSI2-SK";


    public String getTableName() {
        return "Company";
    }



    protected PutItemRequest putRequest(Employee employee) {

        List<AttributeValue> locationHistory = new ArrayList<>();
        for (int i = 0; i < employee.historyLocation().size() ; i++) {
            locationHistory.add(AttributeValue.fromS(employee.historyLocation().get(i)));
        }


        // o JSON tem de ter todos os campos preenchidos menos os GSI's porque recebem os valores de outros campos
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK_COL, AttributeValue.builder().s("#" + employee.PK()).build());
        item.put(SK_COL, AttributeValue.builder().s("#EMPLOYEE" + employee.SK()).build());
        item.put(NAME_COL, AttributeValue.builder().s(employee.name()).build());
        item.put(SALARY_COL, AttributeValue.builder().n(employee.salary().toString()).build());
        item.put(FUNCTION_COL, AttributeValue.builder().s(employee.function()).build());
        item.put(GSI1_PK, AttributeValue.builder().s("#" + employee.PK()).build());
        item.put(GSI1_SK, AttributeValue.builder().s(employee.name()).build());
        item.put(LOCATION, AttributeValue.builder().s(employee.location()).build());
        item.put(HISTORY_LOCATION, AttributeValue.builder().l(locationHistory).build());
        item.put(GSI2_PK, AttributeValue.builder().s(employee.location()).build());
        item.put(GSI2_SK, AttributeValue.builder().s("#EMPLOYEE" + employee.SK()).build());


        return PutItemRequest.builder()
                .tableName(getTableName())
                .item(item)
                .build();
    }

    protected UpdateItemRequest updateTableItem(String nameOfDepartment, String employeeId, Employee employee){

        Map<String,AttributeValue> itemKey = new HashMap<>();
        itemKey.put(PK_COL, AttributeValue.builder().s("#"+ nameOfDepartment).build());
        itemKey.put(SK_COL, AttributeValue.builder().s("#EMPLOYEE" + employeeId).build());


        //Só pode atualizar atributos, não pode atualizar PK nem SK, o dynamoDB não deixa uma vez que é a sua Key
        //O GSI1-PK e GSI1-SK consegue atualizar, mas não coloquei porque assumo que deve ser sempre os mesmos.

        HashMap<String,AttributeValueUpdate> updatedValues = new HashMap<>();
        if(employee.name() != null) {
            updatedValues.put(NAME_COL, AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(employee.name()).build())
                    .action(AttributeAction.PUT)
                    .build());
        }
        if(employee.salary() != null) {
            updatedValues.put(SALARY_COL, AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().n(employee.salary().toString()).build()) //o dynamodb so trabalha com strings, por isso temos de converter
                    .action(AttributeAction.PUT)
                    .build());
        }
        if(employee.function() != null) {
            updatedValues.put(FUNCTION_COL, AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(employee.function()).build())
                    .action(AttributeAction.PUT)
                    .build());
        }
        if(employee.location() != null) {
            updatedValues.put(LOCATION, AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(employee.location()).build())
                    .action(AttributeAction.PUT)
                    .build());
            updatedValues.put(GSI2_PK, AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(employee.location()).build())
                    .action(AttributeAction.PUT)
                    .build());
        }

        return UpdateItemRequest.builder()
                .tableName(getTableName())
                .key(itemKey)
                .attributeUpdates(updatedValues)
                .build();

    }


    protected GetItemRequest getRequest(String nameOfDepartment, String employeeId) {

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PK_COL, AttributeValue.builder().s("#"+ nameOfDepartment).build());
        key.put(SK_COL, AttributeValue.builder().s("#EMPLOYEE" + employeeId).build());

        return GetItemRequest.builder()
                .tableName(getTableName())
                .key(key)
                //.attributesToGet()
                .build();
    }

    protected DeleteItemRequest deleteEmployeeFromTable(String nameOfDepartment, String employeeId) {

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(PK_COL, AttributeValue.builder().s("#"+ nameOfDepartment).build());
        key.put(SK_COL, AttributeValue.builder().s("#EMPLOYEE" + employeeId).build());

        return  DeleteItemRequest.builder()
                .tableName(getTableName())
                .key(key)
                .build();

    }

}
