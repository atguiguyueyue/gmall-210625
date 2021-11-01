package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@RestController=@Controller+@ResponseBody
@RestController
public class ControllerTest {

    @RequestMapping("test")
//    @ResponseBody
    public String test(){
        System.out.println("111");
        return "success";
    }

    @RequestMapping("test2")
    public String test3(
            @RequestParam("name") String na,
            @RequestParam("age") Integer a){
        System.out.println("123");
        return "name:" + na + " age:" + a;
    }

}
