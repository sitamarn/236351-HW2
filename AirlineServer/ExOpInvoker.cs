using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceModel.Dispatcher;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class ExOpInvoker: IOperationInvoker
    {
        private IOperationInvoker invoker;
        CacheData cache = null;

        public ExOpInvoker(CacheData inputCache, IOperationInvoker invoker)
        {
            this.invoker = invoker;
            cache = inputCache;
        }

        public object[] AllocateInputs()
        {
            return invoker.AllocateInputs();
        }

        public object Invoke(object instance, object[] inputs, out object[] outputs)
        {
            outputs = new object[1];
            outputs[0] = new object();

            string key = inputs[0] + " " + inputs[1] + " " + (DateTime)inputs[2];
            List<string> companies = (List<string>)inputs[3];
            for (int i = 0; i < companies.Count; i++)
            {
                key += " " + companies[i];
            }
            Object result = cache.getDataFromCache(key);

            if (result == null)
            {
                result = invoker.Invoke(instance, inputs, out outputs);
                cache.insert(key, (List<Trip>)result);
            }

            return result;
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
