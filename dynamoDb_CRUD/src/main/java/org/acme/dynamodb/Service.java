package org.acme.dynamodb;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;


@ApplicationScoped
public class Service extends AbstractService {

    @Inject
    DynamoDbClient dynamoDB;

    public List<Employee> queryWorkersByDepartment(String department) {

            HashMap<String,String> attrNameAlias = new HashMap<String,String>();
            attrNameAlias.put("#PK", "PK");
            attrNameAlias.put("#SK", "SK");

            // Set up mapping of the partition name with the value.
            HashMap<String, AttributeValue> attrValues = new HashMap<>();
            attrValues.put(":PK", AttributeValue.builder()
                    .s("#" + department)
                    .build());
            attrValues.put(":SK", AttributeValue.builder()
                    .s("#EMPLOYEE")
                    .build());

            QueryRequest queryReq = QueryRequest.builder()
                    .tableName("Company")
                    .keyConditionExpression("#PK = :PK And begins_with(#SK, :SK)")
                    .expressionAttributeNames(attrNameAlias)
                    .expressionAttributeValues(attrValues)
                    .build();


                QueryResponse response = dynamoDB.query(queryReq);
                return response.items().stream().map(Employee::converter).collect(Collectors.toList());
        }

    public List<Employee> queryWorkersByDepartmentByName(String nameOfDepartment, String name) {

        HashMap<String,String> attrNameAlias = new HashMap<String,String>();
        attrNameAlias.put("#PK", "GSI1-PK");
        attrNameAlias.put("#SK", "GSI1-SK");

        // Set up mapping of the partition name with the value.
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":GSI_PK", AttributeValue.builder()
                .s("#" + nameOfDepartment)
                .build());
        attrValues.put(":GSI_SK", AttributeValue.builder()
                .s(name)
                .build());

        QueryRequest queryReq = QueryRequest.builder()
                .tableName("Company")
                .indexName("Employees_by_department")
                .keyConditionExpression("#PK = :GSI_PK And begins_with(#SK, :GSI_SK)")
                .expressionAttributeNames(attrNameAlias)
                .expressionAttributeValues(attrValues)
                .build();


        QueryResponse response = dynamoDB.query(queryReq);
        return response.items().stream().map(Employee::converter).collect(Collectors.toList());
    }

    public List<Employee> queryWorkersByCity(String city) {
        HashMap<String,String> attrNameAlias = new HashMap<String,String>();
        attrNameAlias.put("#PK", "GSI2-PK");


        // Set up mapping of the partition name with the value.
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":GSI_PK", AttributeValue.builder()
                .s(city)
                .build());


        QueryRequest queryReq = QueryRequest.builder()
                .tableName("Company")
                .indexName("Employees_by_localization")
                .keyConditionExpression("#PK = :GSI_PK ")
                .expressionAttributeNames(attrNameAlias)
                .expressionAttributeValues(attrValues)
                .build();


        QueryResponse response = dynamoDB.query(queryReq);

        return response.items().stream().map(Employee::converter).collect(Collectors.toList());
    }

    public Employee get(String nameOfDepartment, String employeeId) {

        return Employee.converter(dynamoDB.getItem(getRequest(nameOfDepartment, employeeId)).item());
    }

    public Employee add(Employee employee) {
        dynamoDB.putItem(putRequest(employee));
        return get(employee.PK(), employee.SK());
    }

    public Employee updateEmployee(String nameOfDeparment, String employeeId, Employee employee) {

        dynamoDB.updateItem(updateTableItem(nameOfDeparment, employeeId, employee));

        return Employee.converter(dynamoDB.getItem(getRequest(nameOfDeparment,employeeId)).item());
    }

    public String deleteEmployee(String nameOfDepartment, String employeeId) {
        dynamoDB.deleteItem(deleteEmployeeFromTable(nameOfDepartment, employeeId));

        return employeeId + " has been deleted with success.";
    }



}