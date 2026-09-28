package com.easy.etl.flink.http;

import cn.hutool.core.io.FileUtil;
import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.easy.etl.flink.http.model.GatewayResult;
import com.easy.etl.flink.http.model.SessionInfo;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JobIdTest {
    public static void main(String[] args) throws IOException {
        String sqlScript = FileUtils.readFileToString(FileUtil.file("D:\\IdeaWorks\\BigDataETL\\EasyETL\\flinkcdc\\syc.sql"), "utf-8");
        // 1. 按行分割，过滤掉以 "--" 开头的注释行
        String validSql = Arrays.stream(sqlScript.split("\\n"))
                .map(String::trim)
                .filter(line -> !line.startsWith("--") && !line.isEmpty()) // 过滤注释行和空行
                .collect(Collectors.joining("\n"));

        // 2. 按分号拆分，并去除拆分后产生的空字符串
        List<String> sqlList = Arrays.stream(validSql.split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
        SqlGatewayClient client = new SqlGatewayClient("jdbc:flink:http://flink01:32325");
        String handle = client.openSession().getSessionHandle();

        for (String sql : sqlList) {
            try {
              GatewayResult result=  client.execute(handle, sql+";");
              System.out.println(result.getJobId());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(sql);
        }
        client.closeSession(handle);
    }
}
