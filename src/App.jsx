import React, { useState, useEffect, useRef } from 'react';
import { Play, Database, Server, Users, ArrowRight, Info, Trash2, Send, Activity, Plus, BookOpen, X } from 'lucide-react';

const TOPIC_NAME = "web-clicks";
const PARTITIONS = [0, 1, 2];

export default function App() {
  const [messages, setMessages] = useState([]);
  const [nextOffsets, setNextOffsets] = useState({ 0: 0, 1: 0, 2: 0 });
  const [consumers, setConsumers] = useState([{ id: 'C1', groupId: 'Analytics', assignedPartitions: [0, 1, 2] }]);
  const [groupOffsets, setGroupOffsets] = useState({ 'Analytics': { 0: 0, 1: 0, 2: 0 }, 'Notifications': { 0: 0, 1: 0, 2: 0 } });
  const [consumerLogs, setConsumerLogs] = useState({});
  const [inputText, setInputText] = useState("");
  const [autoProduce, setAutoProduce] = useState(false);
  const [autoPoll, setAutoPoll] = useState(false);
  const [isGuideOpen, setIsGuideOpen] = useState(false);
  const messagesEndRef = useRef(null);

  useEffect(() => {
    const groups = ['Analytics', 'Notifications'];
    let updatedConsumers = [...consumers];
    groups.forEach(groupId => {
      const groupConsumers = updatedConsumers.filter(c => c.groupId === groupId);
      if (groupConsumers.length === 0) return;
      groupConsumers.forEach(c => c.assignedPartitions = []);
      PARTITIONS.forEach((partitionId, index) => {
        const consumerIndex = index % groupConsumers.length;
        groupConsumers[consumerIndex].assignedPartitions.push(partitionId);
      });
    });
    setConsumers(updatedConsumers);
  }, [consumers.length]);

  const produceMessage = (text) => {
    if (!text.trim()) return;
    const partition = Math.floor(Math.random() * PARTITIONS.length);
    const offset = nextOffsets[partition];
    const newMessage = { id: Math.random().toString(36).substr(2, 9), partition, offset, text };
    setMessages(prev => [...prev, newMessage]);
    setNextOffsets(prev => ({ ...prev, [partition]: prev[partition] + 1 }));
    setInputText("");
  };

  const pollMessages = () => {
    let newGroupOffsets = { ...groupOffsets };
    let newConsumerLogs = { ...consumerLogs };
    let processedAny = false;
    consumers.forEach(consumer => {
      const groupId = consumer.groupId;
      if (!newConsumerLogs[consumer.id]) newConsumerLogs[consumer.id] = [];
      consumer.assignedPartitions.forEach(partition => {
        const currentOffset = newGroupOffsets[groupId][partition];
        const unreadMessages = messages.filter(m => m.partition === partition && m.offset >= currentOffset);
        if (unreadMessages.length > 0) {
          processedAny = true;
          unreadMessages.forEach(m => {
            newConsumerLogs[consumer.id].unshift({ text: m.text, partition: m.partition, offset: m.offset, time: new Date().toLocaleTimeString() });
          });
          if (newConsumerLogs[consumer.id].length > 5) newConsumerLogs[consumer.id] = newConsumerLogs[consumer.id].slice(0, 5);
          const maxOffset = Math.max(...unreadMessages.map(m => m.offset));
          newGroupOffsets[groupId][partition] = maxOffset + 1;
        }
      });
    });
    if (processedAny) { setGroupOffsets(newGroupOffsets); setConsumerLogs(newConsumerLogs); }
  };

  useEffect(() => {
    let interval;
    if (autoProduce) interval = setInterval(() => { produceMessage(["user_login", "add_to_cart", "checkout", "page_view"][Math.floor(Math.random() * 4)]); }, 1500);
    return () => clearInterval(interval);
  }, [autoProduce, nextOffsets]);

  useEffect(() => {
    let interval;
    if (autoPoll) interval = setInterval(pollMessages, 1000);
    return () => clearInterval(interval);
  }, [autoPoll, messages, groupOffsets, consumers]);

  const addConsumer = (groupId) => setConsumers([...consumers, { id: `C${consumers.length + 1}`, groupId, assignedPartitions: [] }]);
  const removeConsumer = (id) => setConsumers(consumers.filter(c => c.id !== id));
  const clearData = () => { setMessages([]); setNextOffsets({ 0: 0, 1: 0, 2: 0 }); setGroupOffsets({ 'Analytics': { 0: 0, 1: 0, 2: 0 }, 'Notifications': { 0: 0, 1: 0, 2: 0 } }); setConsumerLogs({}); };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 p-4 md:p-8 font-sans overflow-x-hidden relative">
      
      {/* SIDEBAR OVERLAY */}
      {isGuideOpen && (
        <div className="fixed inset-0 bg-black/50 z-40 transition-opacity" onClick={() => setIsGuideOpen(false)} />
      )}

      {/* LEFT SIDEBAR GUIDE */}
      <div className={`fixed top-0 left-0 h-full w-96 max-w-[90vw] bg-slate-900 border-r-4 border-red-600 shadow-2xl z-50 transform transition-transform duration-300 ease-in-out ${isGuideOpen ? 'translate-x-0' : '-translate-x-full'} overflow-y-auto`}>
        <div className="p-6">
          <div className="flex justify-between items-start mb-6 border-b border-slate-700 pb-4">
            <h2 className="text-2xl font-black text-red-500 uppercase tracking-wider">Deadpool's Guide<br/><span className="text-sm text-slate-400 normal-case font-normal">to not destroying the database</span></h2>
            <button onClick={() => setIsGuideOpen(false)} className="text-slate-400 hover:text-white bg-slate-800 hover:bg-red-600 p-2 rounded transition-colors"><X className="w-5 h-5" /></button>
          </div>
          
          <div className="space-y-6 text-sm text-slate-300 leading-relaxed">
            <div>
              <h3 className="text-red-400 font-bold text-lg mb-2">1. The Producer (The Loudmouth)</h3>
              <p>Think of the Producer as me on a Tuesday. It just shouts things into the void (Kafka). "User logged in!" "User bought a chimichanga!" It doesn't care who's listening. It just drops the message and leaves.</p>
            </div>
            <div>
              <h3 className="text-red-400 font-bold text-lg mb-2">2. Topics & Partitions (The Filing Cabinets)</h3>
              <p>A <strong>Topic</strong> is just a label, like "Web-Clicks". But if a million people click at once, one server melts. So, Kafka splits the Topic into <strong>Partitions</strong> (Buckets). Messages get thrown into these buckets. Once a message is in a bucket, it gets an ID called an <strong>Offset</strong>. <em>And it never changes.</em> Immutable. Like my love for Bea Arthur.</p>
            </div>
            <div>
              <h3 className="text-red-400 font-bold text-lg mb-2">3. Consumer Groups (The Mercenaries)</h3>
              <p>This is where the magic happens. A <strong>Consumer</strong> reads the messages. A <strong>Consumer Group</strong> is a team of mercenaries working together.</p>
              <ul className="list-disc pl-5 mt-2 space-y-1 text-slate-400">
                <li>If you have 3 Partitions and 1 Consumer, that guy does all the work.</li>
                <li>If you add a 2nd Consumer, they split the buckets! <strong>(Load Balancing!)</strong></li>
                <li>If you add a 4th Consumer... he sits on the couch and does nothing, because there are only 3 buckets.</li>
              </ul>
            </div>
            <div className="bg-slate-950 p-4 border border-red-900 rounded-lg">
              <h3 className="text-red-500 font-bold mb-1">The "Ah-Ha!" Moment</h3>
              <p>Look at the UI. The "Analytics" group and the "Notifications" group get the EXACT SAME MESSAGES. Kafka doesn't delete a message when Analytics reads it. It just keeps it. Both teams track their own <strong>Offset</strong> (bookmark) so they know where they left off.</p>
            </div>
          </div>
        </div>
      </div>

      {/* HEADER */}
      <div className="max-w-7xl mx-auto mb-8 flex flex-col md:flex-row justify-between items-center gap-4">
        <div className="flex items-center gap-4">
          <button onClick={() => setIsGuideOpen(true)} className="bg-red-600 hover:bg-red-500 text-white px-4 py-2 rounded-lg font-black tracking-wide uppercase flex items-center gap-2 shadow-[0_0_15px_rgba(220,38,38,0.5)] transition-all transform hover:scale-105">
            <BookOpen className="w-5 h-5" /> Help Me!
          </button>
          <div>
            <h1 className="text-3xl font-bold text-white flex items-center gap-3"><Server className="w-8 h-8 text-orange-500" /> Kafka Simulator</h1>
          </div>
        </div>
        <div className="flex gap-3">
          <button onClick={() => setAutoProduce(!autoProduce)} className={`px-4 py-2 rounded-md font-medium flex items-center gap-2 transition-colors ${autoProduce ? 'bg-orange-600 text-white' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`}><Activity className="w-4 h-4" /> {autoProduce ? 'Stop Producer' : 'Auto Produce'}</button>
          <button onClick={() => setAutoPoll(!autoPoll)} className={`px-4 py-2 rounded-md font-medium flex items-center gap-2 transition-colors ${autoPoll ? 'bg-green-600 text-white' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`}><Play className="w-4 h-4" /> {autoPoll ? 'Stop Polling' : 'Auto Poll'}</button>
          <button onClick={clearData} className="p-2 bg-slate-800 hover:bg-red-900/50 hover:text-red-400 rounded-md transition-colors"><Trash2 className="w-5 h-5" /></button>
        </div>
      </div>

      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* PRODUCER */}
        <div className="lg:col-span-3 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg">
            <h2 className="text-xl font-semibold mb-4 flex items-center gap-2 text-white"><Send className="w-5 h-5 text-orange-400" /> Producer</h2>
            <div className="flex gap-2">
              <input type="text" value={inputText} onChange={(e) => setInputText(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && produceMessage(inputText)} placeholder="e.g., login" className="flex-1 bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm text-white" />
              <button onClick={() => produceMessage(inputText)} className="bg-orange-500 hover:bg-orange-600 text-white p-2 rounded"><ArrowRight className="w-5 h-5" /></button>
            </div>
          </div>
        </div>

        {/* BROKER */}
        <div className="lg:col-span-5 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg h-full">
            <h2 className="text-xl font-semibold mb-4 flex items-center gap-2 text-white"><Database className="w-5 h-5 text-blue-400" /> Broker</h2>
            <div className="bg-slate-950 border border-slate-800 rounded-lg p-4 mt-2">
              <span className="font-mono text-lg text-blue-400 font-bold block mb-4 border-b border-slate-800 pb-2">Topic: {TOPIC_NAME}</span>
              <div className="flex flex-col gap-4">
                {PARTITIONS.map(partition => (
                  <div key={partition} className="relative">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1"><span>Partition {partition}</span></div>
                    <div className="h-16 bg-slate-900 border border-slate-700 rounded flex items-center p-2 overflow-x-auto gap-2 no-scrollbar">
                      {messages.filter(m => m.partition === partition).map(msg => (
                        <div key={msg.id} className="bg-orange-500/20 border border-orange-500/50 text-orange-200 px-3 py-1 rounded text-xs whitespace-nowrap flex flex-col items-center min-w-[80px]">
                          <span className="font-bold text-[10px] text-orange-400">o:{msg.offset}</span><span className="font-mono truncate">{msg.text}</span>
                        </div>
                      ))}
                      <div ref={messagesEndRef} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* CONSUMERS */}
        <div className="lg:col-span-4 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg min-h-full">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-semibold flex items-center gap-2 text-white"><Users className="w-5 h-5 text-green-400" /> Consumers</h2>
              {!autoPoll && <button onClick={pollMessages} className="bg-green-600 hover:bg-green-500 text-white text-xs px-3 py-1.5 rounded">Manual Poll</button>}
            </div>
            {['Analytics', 'Notifications'].map(groupId => {
              const groupConsumers = consumers.filter(c => c.groupId === groupId);
              return (
                <div key={groupId} className={`mt-4 border ${groupId === 'Analytics' ? 'border-green-500/30 bg-green-950/10' : 'border-purple-500/30 bg-purple-950/10'} rounded-lg p-3`}>
                  <div className="flex justify-between items-center mb-3">
                    <h3 className={`font-bold ${groupId === 'Analytics' ? 'text-green-400' : 'text-purple-400'} flex items-center gap-2`}>Group: {groupId}</h3>
                    <button onClick={() => addConsumer(groupId)} className="p-1 hover:bg-slate-800 rounded text-slate-400"><Plus className="w-4 h-4" /></button>
                  </div>
                  <div className="space-y-3">
                    {groupConsumers.map(consumer => (
                      <div key={consumer.id} className="bg-slate-950 border border-slate-800 rounded p-3 relative">
                        <div className="flex justify-between items-start mb-2">
                          <div className="flex items-center gap-2">
                            <span className="font-mono font-bold text-slate-200">{consumer.id}</span>
                            <div className="flex gap-1">{consumer.assignedPartitions.length > 0 ? consumer.assignedPartitions.map(p => <span key={p} className="text-[10px] bg-slate-800 text-slate-300 px-1.5 py-0.5 rounded">P:{p}</span>) : <span className="text-[10px] bg-red-900/50 text-red-300 px-1.5 py-0.5 rounded">Idle</span>}</div>
                          </div>
                          <button onClick={() => removeConsumer(consumer.id)} className="text-slate-500 hover:text-red-400"><Trash2 className="w-3.5 h-3.5" /></button>
                        </div>
                        <div className="h-16 bg-slate-900 rounded p-2 overflow-y-auto text-xs font-mono">
                          {consumerLogs[consumer.id]?.map((log, i) => <div key={i} className="text-slate-300 border-b border-slate-800/50 last:border-0 py-0.5">"{log.text}" <span className="text-slate-500 text-[10px] ml-2">p{log.partition}:o{log.offset}</span></div>)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}