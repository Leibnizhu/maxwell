package io.gitlab.leibnizhu.maxwell.producer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HiveConfigTest {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testStrictNone(){
        Properties prop = new Properties();
        prop.setProperty("strict", "None");
        new HiveConfig(prop);
    }

    @Test
    public void testStrictDatabase(){
        Properties prop = new Properties();
        prop.setProperty("strict", "dataBaSe");
        new HiveConfig(prop);
    }

    @Test
    public void testStrictTable(){
        Properties prop = new Properties();
        prop.setProperty("strict", "tABLe");
        new HiveConfig(prop);
    }

    @Test
    public void testStrictWrong(){
        Properties prop = new Properties();
        prop.setProperty("strict", "asdf4");
        new HiveConfig(prop);
    }

    @Test
    public void testTableRule(){
        Properties prop = new Properties();
        prop.setProperty("rule", "AAaa.Bbb->Ccc.Ddd,A;AAaa2.Bbb2->Ccc2.Ddd2,M;AAaa3.Bbb3->Ccc3.Ddd3;AAaa4.Bbb4->Ccc4.Ddd4,XX");
        new HiveConfig(prop);
    }

    @Test
    public void testDatabaseRule(){
        Properties prop = new Properties();
        prop.setProperty("rule", "AAaa->Ccc;AAaa2->Ccc2.M;AAaa4.->Ccc4.");
        new HiveConfig(prop);
    }

    @Test
    public void testBothRule(){
        Properties prop = new Properties();
        prop.setProperty("rule", "AAaa->Ccc,A;AAaa.Bbb->Ccc.Ddd,A;AAaa2.Bbb2->Ccc2.Ddd2,M,id;AAaa2->Ccc2;AAaa3.Bbb3->Ccc3.Ddd3;AAaa4.Bbb4->Ccc4.Ddd4,XX;AAaa3->Ccc3;AAaa4.->Ccc4.");
        HiveConfig config = new HiveConfig(prop);
        log.info("Strict:" + config.strict());
        log.info("Contains Database AAaa2:" + config.containDatabase("AAaa2"));
        log.info("Contains Database Ccc5:" + config.containDatabase("Ccc5"));
        log.info("Contains Database AAaa2.Bbb2:" + config.containTable("AAaa2", "Bbb2"));
        log.info("Contains Database Ccc5.Bbb2:" + config.containTable("Ccc5", "Bbb2"));
        log.info("Contains Database AAaa2.Bbb5:" + config.containTable("AAaa2", "Ddd5"));
    }
}
