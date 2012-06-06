package ptest.com.g414.st8.count;

import java.util.ArrayList;
import java.util.List;

import com.g414.dgen.EntityGenerator;
import com.g414.dgen.field.Field;
import com.g414.dgen.field.Fields;
import com.g414.hash.impl.MurmurHash;

public class CounterEntity {
    public static EntityGenerator createGenerator() {
        List<Field<?>> fields = new ArrayList<Field<?>>();
        fields.add(Fields.getIdField("id"));

        return new EntityGenerator(new MurmurHash(), fields);
    }
}