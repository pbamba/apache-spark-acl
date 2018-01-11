package fr.alg.acl.domain

import java.io.StringWriter

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

trait Jsonable {

  def toJson: String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)

    val out = new StringWriter
    mapper.writeValue(out, this).toString
    out.toString
  }
}
