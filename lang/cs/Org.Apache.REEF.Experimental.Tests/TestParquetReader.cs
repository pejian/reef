using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using Org.Apache.REEF.Experimental.ParquetReader;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Experimental.Tests
{
    public sealed class TestParquetReader
    {

        [DataContract(Name = "User", Namespace = "parquetreader")]
        internal class User
        {
            [DataMember(Name = "name")]
            public string name { get; set; }

            [DataMember(Name = "age")]
            public int age { get; set; }

            [DataMember(Name = "favorite_color")]
            public string favorite_color { get; set; }
        }

        [Fact]
        public void TestExists()
        {
            string jarPath = "E:/reef-fork/reef/lang/java/reef-experimental/target/reef-experimental-0.16.0-SNAPSHOT-jar-with-dependencies.jar";
            string parquetPath = "E:/reef-fork/reef/lang/java/reef-experimental/src/test/resources/file.parquet";
            string avroPath = "E:/user2.avro";
            System.Diagnostics.Process clientProcess = new System.Diagnostics.Process();
            clientProcess.StartInfo.FileName = "java";
            clientProcess.StartInfo.Arguments = @"-cp " + jarPath + " org.apache.reef.experimental.parquet.ParquetReader " + parquetPath + " " + avroPath;
            clientProcess.Start();
            clientProcess.WaitForExit();
            int code = clientProcess.ExitCode;

            System.Diagnostics.Debug.WriteLine(code);


            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { "ParquetReader Test" });
            cb.BindNamedParameter<PathString, string>(GenericType<PathString>.Class, "E:/user2.avro");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var reader = (ParquetReader.ParquetReader)injector.GetInstance(typeof(ParquetReader.ParquetReader));

            var list = reader.read<User>();

            foreach (var obj in list)
            {
                System.Diagnostics.Debug.WriteLine(obj.age);
            }


        }
    }
}
