using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceModel.Dispatcher;
using System.Text;
using System.Threading.Tasks;

namespace FlightSearchServer
{
    class ExOpInvoker: IOperationInvoker
    {
        private IOperationInvoker invoker;
        string fileName;

        public ExOpInvoker(string FileName, IOperationInvoker invoker)
        {
            this.invoker = invoker;
            fileName = FileName;
        }

        public object[] AllocateInputs()
        {
            return invoker.AllocateInputs();
        }

        public object Invoke(object instance, object[] inputs, out object[] outputs)
        {
            outputs = new object[1];
            outputs[0] = new object();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string inputLog = "Query paramemters:\r\nSrc: " + inputs[0] + ", Dst: " + inputs[1] + ", Date: " + inputs[2];
            if (inputs.Length > 3) inputLog += ", Sellers:";
            for (int i = 3; i < inputs.Length; i++)
            {
                inputLog += " " + inputs[i];
            }

            inputLog += "\r\n";

            sw.Stop(); // hammertime!

            double duration = sw.Elapsed.TotalMilliseconds;

            string log = inputLog + "time: " + duration.ToString() + " msec\r\n";

            using (StreamWriter writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine(log);
            }

            return invoker.Invoke(instance, inputs, out outputs);
        }

        public IAsyncResult InvokeBegin(object instance, object[] inputs, AsyncCallback callback, object state)
        {
            return invoker.InvokeBegin(instance, inputs, callback, state);
        }

        public object InvokeEnd(object instance, out object[] outputs, IAsyncResult result)
        {
            return invoker.InvokeEnd(instance, out outputs, result);
        }

        public bool IsSynchronous
        {
            get { return invoker.IsSynchronous; }
        }
    }
}
