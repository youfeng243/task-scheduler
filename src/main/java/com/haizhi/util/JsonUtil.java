package com.haizhi.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;


/**
 * @author youfeng
 */
public class JsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.getSerializationConfig().withDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        objectMapper.getDeserializationConfig().set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String objectToJson(Object o) {
        //o = (Object)"Test Test Test";
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            logger.error("object:{} to json error:{}.", o, ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    public static <T> T jsonToObject(String json, Class<T> className) {
        if (StringUtils.isEmpty(json)) {
            return null;
        } else {
            try {
                return objectMapper.readValue(json, className);
            } catch (IOException e) {
                logger.error("json:{} to object error:{}.", json, ExceptionUtils.getStackTrace(e));
                return null;
            }
        }
    }

    public static <T> T utf8BytesToBean(byte[] content, Class<T> className) {
        if (null == content) {
            return null;
        } else {
            try {
                return objectMapper.readValue(new String(content, "utf-8"), className);
            } catch (IOException e) {
                //logger.error("json:{} to object error:{}.", json, ExceptionUtils.getStackTrace(e));
                return null;
            }
        }
    }

    public static <T> T jsonToObject(String json, TypeReference<T> typeReference) {
        if (StringUtils.isEmpty(json)) {
            return null;
        } else {
            try {
                return (T) objectMapper.readValue(json, typeReference);
            } catch (IOException e) {
                logger.error("json:{} to object error:{}.", json, ExceptionUtils.getStackTrace(e));
                return null;
            }
        }
    }
}
