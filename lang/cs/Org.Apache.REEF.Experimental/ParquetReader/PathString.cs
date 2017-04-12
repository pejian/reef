using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Experimental.ParquetReader
{
    [NamedParameter("Path to parquet file", "path", "")]
    public class PathString : Name<string>
    {
    }
}
