package org.apache.wayang.api.json.springboot

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
@EnableAutoConfiguration(excludeName = Array("org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration"))
class BootConfig

object SpringBootApplication extends App {
  SpringApplication.run(classOf[BootConfig])
}
