package org.acme.dynamodb;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Controller {


    @Inject
    Service service;

    //EXAMPLE : localhost:8080/HR
    @GET
    @Path("{nameOfDepartment}")
    public List<Employee> getWorkersByDepartment(String nameOfDepartment){
        return service.queryWorkersByDepartment(nameOfDepartment);
    }

    //EXAMPLE : localhost:8080/HR/Bruno
    @GET
    @Path("/{nameOfDepartment}/{name}")
    public List<Employee> getWorkersByDepartmentWithTheSameName(String nameOfDepartment, String name){
        return service.queryWorkersByDepartmentByName(nameOfDepartment, name);
    }

    @GET
    @Path(("city={city}"))
    public List<Employee> getWorkerByCity(String city){
        return service.queryWorkersByCity(city);
    }

    //EXAMPLE : localhost:8080/get/HR/3
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("get/{nameOfDepartment}/{employeeId}")
    public Employee getSingle(String nameOfDepartment, String employeeId) {
        return service.get(nameOfDepartment, employeeId);
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Employee add(Employee employee) {
        return service.add(employee);

    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{nameOfDepartment}/{employeeId}")
    public Employee updateEmployee(String nameOfDepartment, String employeeId, Employee employee ){
        return service.updateEmployee(nameOfDepartment, employeeId, employee);
    }

    @DELETE
    @Path("{nameOfDepartment}/{employeeId}")
    public String deleteEmployee(String nameOfDepartment, String employeeId){
        return service.deleteEmployee(nameOfDepartment, employeeId);
    }
}