package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;
import org.example.events.CSVEventConverter;

public class Equals implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter, Schema schema) {
        String attributeValue = schema.getValue(attribute, event);
        return attributeValue.equals(parameter.toString());
    }

    public static void main(String[] args) {
        // Create the CSV event converter
        String csvFilePath = "src/main/resources/data/1m_linear_accel.csv"; // Replace with your CSV file path
        String schemaFilePath = "src/main/resources/domain/linear_accel.conf"; // Replace with your schema file path
        CSVEventConverter converter = new CSVEventConverter(csvFilePath, schemaFilePath);

        // 创建 Schema 实例
        Schema schema = new Schema(schemaFilePath);

        // 创建属性
        Attribute speedAttribute = new Attribute("SPEED");

        // 创建参数
        Object equalsParameter = "32";

        // 使用谓词
        Predicate equals = new Equals();

        // 遍历事件并应用谓词，输出前 20 个结果
        int count = 0;
        for (PointEvent event : converter) {
            if (count >= 200) break;
            boolean equalsResult = equals.test(event, speedAttribute, equalsParameter, schema);
            if (equalsResult) {
                System.out.println("Event payload: " + event.getPayload());
                System.out.println("Equals result: " + equalsResult);

            }
            count++;
        }
    }
}
