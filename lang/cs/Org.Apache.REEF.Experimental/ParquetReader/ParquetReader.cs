using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Experimental.ParquetReader
{
    public class ParquetReader
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ParquetReader));

        private readonly string _avroPath;
        private readonly string _parquetPath;

        [Inject]
        private ParquetReader([Parameter(typeof(PathString))] string path)
        {
            _avroPath = path;
        }

        public IEnumerable<T> read<T> () {
            Stream stream = new FileStream(_avroPath, FileMode.Open);
            using (var reader = AvroContainer.CreateReader<T>(stream))
            {
                using (var streamReader = new SequentialReader<T>(reader))
                {
                    return streamReader.Objects;
                }
            }
        }

    }
}
