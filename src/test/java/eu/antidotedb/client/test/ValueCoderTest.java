package eu.antidotedb.client.test;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ValueCoderTest extends AbstractAntidoteTest {

    @Test
    public void staticReadWrite() {

        NoTransaction tx = antidoteClient.noTransaction();

        ValueCoder<UserId> userIdCoder = ValueCoder.stringCoder(UserId::getId, UserId::new);
        SetKey<UserId> userSet = Key.set("users", userIdCoder);

        UserId user1 = new UserId("user1");
        UserId user2 = new UserId("user2");
        // update the user set
        bucket.update(tx, userSet.addAll(user1, user2));

        // read the user set
        List<UserId> value = bucket.read(tx, userSet);

        assertEquals(Arrays.asList(user1, user2), value);


    }

}

class UserId {
    private String id;

    public UserId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserId userId = (UserId) o;
        return Objects.equals(id, userId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
