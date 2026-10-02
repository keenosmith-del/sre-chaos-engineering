import http from 'k6/http';
import {check,sleep} from 'k6';
import {Counter} from 'k6/metrics';
const completed=new Counter('confirmed_orders');
const profiles={baseline:{vus:1,duration:'30s'},concurrent:{vus:4,duration:'30s'},sustained:{vus:2,duration:'360s'},burst:{vus:8,duration:'10s'},recovery:{vus:1,duration:'20s'}};
export const options={...profiles[__ENV.PROFILE||'baseline'],thresholds:{http_req_failed:['rate<0.8']}};
export default function(){
 const key=`k6-${__ENV.PROFILE}-${__VU}-${__ITER}-${Date.now()}`;
 const response=http.post('http://gateway:8080/api/orders',JSON.stringify({product:'mouse',quantity:1,decline:false}),{headers:{'Content-Type':'application/json','Idempotency-Key':key},timeout:'12s'});
 check(response,{'order accepted':r=>r.status===200||r.status===201});
 if(response.status===201||response.status===200){const o=response.json();if(o.state==='CONFIRMED')completed.add(1);}
 sleep(1);
}
