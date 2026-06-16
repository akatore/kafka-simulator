import React, { useState, useEffect, useRef } from 'react';
import { Play, Database, Server, Users, ArrowRight, Info, Settings, Trash2, Send, Activity, Plus, BookOpen, X } from 'lucide-react';

// --- KAFKA CONCEPTS CONSTANTS ---
const TOPIC_NAME = "web-clicks";
const PARTITIONS = [0, 1, 2];

export default function App() {
  // --- STATE ---
  // Messages stored in the broker: { id, partition, offset, text }
  const [messages, setMessages] = useState([]);
  
  // Track next offset for each partition: { 0: 0, 1: 0, 2: 0 }
  const [nextOffsets, setNextOffsets] = useState({ 0: 0, 1: 0, 2: 0 });

  // Consumers: { id, groupId, assignedPartitions: [] }
  const [consumers, setConsumers] = useState([
    { id: 'C1', groupId: 'Analytics', assignedPartitions: [0, 1, 2] },
  ]);

  // Consumer Group Offsets (Tracking where each group left off)
  // { 'Analytics': { 0: 0, 1: 0, 2: 0 } }
  const [groupOffsets, setGroupOffsets] = useState({
    'Analytics': { 0: 0, 1: 0, 2: 0 },
    'Notifications': { 0: 0, 1: 0, 2: 0 }
  });

  // Logs for individual consumers to show what they processed
  const [consumerLogs, setConsumerLogs] = useState({});

  // UI State
  const [inputText, setInputText] = useState("");
  const [autoProduce, setAutoProduce] = useState(false);
  const [autoPoll, setAutoPoll] = useState(false);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const messagesEndRef = useRef(null);

  // --- KAFKA LOGIC: REBALANCING ---
  // Whenever consumers change, re-assign partitions to consumers within their groups.
  useEffect(() => {
    const groups = ['Analytics', 'Notifications'];
    let updatedConsumers = [...consumers];

    groups.forEach(groupId => {
      const groupConsumers = updatedConsumers.filter(c => c.groupId === groupId);
      if (groupConsumers.length === 0) return;

      // Reset assignments for this group
      groupConsumers.forEach(c => c.assignedPartitions = []);

      // Distribute partitions [0, 1, 2] across groupConsumers
      // E.g., 3 partitions, 2 consumers -> C1 gets [0,1], C2 gets [2]
      PARTITIONS.forEach((partitionId, index) => {
        const consumerIndex = index % groupConsumers.length;
        groupConsumers[consumerIndex].assignedPartitions.push(partitionId);
      });
    });

    setConsumers(updatedConsumers);
  }, [consumers.length]); // Re-run when consumer count changes


  // --- KAFKA LOGIC: PRODUCER ---
  const produceMessage = (text) => {
    if (!text.trim()) return;

    // Simulate Producer choosing a partition (Round Robin or Hash). 
    // Here we just pick a random partition for simulation purposes.
    const partition = Math.floor(Math.random() * PARTITIONS.length);
    const offset = nextOffsets[partition];

    const newMessage = {
      id: Math.random().toString(36).substr(2, 9),
      partition,
      offset,
      text
    };

    setMessages(prev => [...prev, newMessage]);
    setNextOffsets(prev => ({ ...prev, [partition]: prev[partition] + 1 }));
    setInputText("");
  };

  // --- KAFKA LOGIC: CONSUMER (POLLING) ---
  const pollMessages = () => {
    let newGroupOffsets = { ...groupOffsets };
    let newConsumerLogs = { ...consumerLogs };
    let processedAny = false;

    consumers.forEach(consumer => {
      const groupId = consumer.groupId;
      if (!newConsumerLogs[consumer.id]) newConsumerLogs[consumer.id] = [];

      consumer.assignedPartitions.forEach(partition => {
        const currentOffset = newGroupOffsets[groupId][partition];
        
        // Find messages in this partition that have an offset >= the group's current offset
        const unreadMessages = messages.filter(m => 
          m.partition === partition && m.offset >= currentOffset
        );

        if (unreadMessages.length > 0) {
          processedAny = true;
          // Process messages
          unreadMessages.forEach(m => {
            newConsumerLogs[consumer.id].unshift({
              text: m.text,
              partition: m.partition,
              offset: m.offset,
              time: new Date().toLocaleTimeString()
            });
          });

          // Keep logs manageable
          if (newConsumerLogs[consumer.id].length > 5) {
             newConsumerLogs[consumer.id] = newConsumerLogs[consumer.id].slice(0, 5);
          }

          // Commit Offset: Advance the group's offset for this partition
          const maxOffset = Math.max(...unreadMessages.map(m => m.offset));
          newGroupOffsets[groupId][partition] = maxOffset + 1;
        }
      });
    });

    if (processedAny) {
      setGroupOffsets(newGroupOffsets);
      setConsumerLogs(newConsumerLogs);
    }
  };

  // Auto-producer effect
  useEffect(() => {
    let interval;
    if (autoProduce) {
      interval = setInterval(() => {
        const events = ["user_login", "add_to_cart", "checkout", "page_view", "btn_click"];
        produceMessage(events[Math.floor(Math.random() * events.length)]);
      }, 1500);
    }
    return () => clearInterval(interval);
  }, [autoProduce, nextOffsets]);

  // Auto-consumer polling effect
  useEffect(() => {
    let interval;
    if (autoPoll) {
      interval = setInterval(() => {
        pollMessages();
      }, 1000);
    }
    return () => clearInterval(interval);
  }, [autoPoll, messages, groupOffsets, consumers]);

  // --- UI ACTIONS ---
  const addConsumer = (groupId) => {
    const id = `C${consumers.length + 1}`;
    setConsumers([...consumers, { id, groupId, assignedPartitions: [] }]);
  };

  const removeConsumer = (id) => {
    setConsumers(consumers.filter(c => c.id !== id));
  };

  const clearData = () => {
    setMessages([]);
    setNextOffsets({ 0: 0, 1: 0, 2: 0 });
    setGroupOffsets({
      'Analytics': { 0: 0, 1: 0, 2: 0 },
      'Notifications': { 0: 0, 1: 0, 2: 0 }
    });
    setConsumerLogs({});
  };


  // --- RENDER HELPERS ---
  const renderEducationalNote = (title, text) => (
    <div className="bg-blue-900/30 border border-blue-500/30 p-3 rounded-lg mb-4 text-sm text-blue-100 flex items-start gap-3">
      <Info className="w-5 h-5 text-blue-400 shrink-0 mt-0.5" />
      <div>
        <strong className="block text-blue-300 mb-1">{title}</strong>
        {text}
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 p-4 md:p-8 font-sans">
      
      {/* HEADER */}
      <div className="max-w-7xl mx-auto mb-8 flex flex-col md:flex-row justify-between items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold text-white flex items-center gap-3">
            <Server className="w-8 h-8 text-orange-500" />
            Apache Kafka Simulator
          </h1>
          <p className="text-slate-400 mt-1">Interactive guide to Topics, Partitions, and Consumer Groups</p>
        </div>
        <div className="flex flex-wrap gap-3 items-center">
          <button 
            onClick={() => setIsSidebarOpen(true)}
            className="px-4 py-2 rounded-md font-bold flex items-center gap-2 transition-all bg-red-600 hover:bg-red-500 text-white shadow-[0_0_15px_rgba(220,38,38,0.4)] hover:shadow-[0_0_25px_rgba(220,38,38,0.6)]"
          >
            <BookOpen className="w-4 h-4" />
            Deadpool's Guide
          </button>
          <button 
            onClick={() => setAutoProduce(!autoProduce)}
            className={`px-4 py-2 rounded-md font-medium flex items-center gap-2 transition-colors ${autoProduce ? 'bg-orange-600 text-white' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`}
          >
            <Activity className="w-4 h-4" />
            {autoProduce ? 'Stop Producer' : 'Auto Produce'}
          </button>
          <button 
            onClick={() => setAutoPoll(!autoPoll)}
            className={`px-4 py-2 rounded-md font-medium flex items-center gap-2 transition-colors ${autoPoll ? 'bg-green-600 text-white' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`}
          >
            <Play className="w-4 h-4" />
            {autoPoll ? 'Stop Polling' : 'Auto Poll'}
          </button>
          <button onClick={clearData} className="p-2 bg-slate-800 hover:bg-red-900/50 hover:text-red-400 rounded-md transition-colors text-slate-300" title="Reset Simulation">
            <Trash2 className="w-5 h-5" />
          </button>
        </div>
      </div>

      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-6">
        
        {/* LEFT COLUMN: PRODUCERS */}
        <div className="lg:col-span-3 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg">
            <h2 className="text-xl font-semibold mb-4 flex items-center gap-2 text-white">
              <Send className="w-5 h-5 text-orange-400" />
              Producer
            </h2>
            {renderEducationalNote(
              "What is a Producer?", 
              "Producers send data (messages/events) to a Kafka Topic. The message is assigned to a specific Partition (often via a key or round-robin)."
            )}
            
            <div className="flex flex-col gap-3">
              <div>
                <label className="text-xs text-slate-400 uppercase font-bold tracking-wider mb-1 block">Topic</label>
                <div className="bg-slate-950 border border-slate-800 p-2 rounded text-slate-300 font-mono text-sm">
                  {TOPIC_NAME}
                </div>
              </div>
              <div>
                <label className="text-xs text-slate-400 uppercase font-bold tracking-wider mb-1 block">Message Event</label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={inputText}
                    onChange={(e) => setInputText(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && produceMessage(inputText)}
                    placeholder="e.g., user_signup"
                    className="flex-1 bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500 transition-colors text-white placeholder-slate-600"
                  />
                  <button
                    onClick={() => produceMessage(inputText)}
                    className="bg-orange-500 hover:bg-orange-600 text-white p-2 rounded transition-colors"
                  >
                    <ArrowRight className="w-5 h-5" />
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* CENTER COLUMN: THE BROKER (TOPICS & PARTITIONS) */}
        <div className="lg:col-span-5 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg h-full">
            <h2 className="text-xl font-semibold mb-4 flex items-center gap-2 text-white">
              <Database className="w-5 h-5 text-blue-400" />
              Kafka Cluster (Broker)
            </h2>
            {renderEducationalNote(
              "Topics & Partitions", 
              "A Topic is a category. Partitions allow a topic to scale across multiple servers. Each message gets an immutable 'Offset' (ID) within its partition."
            )}

            <div className="bg-slate-950 border border-slate-800 rounded-lg p-4 mt-6">
              <div className="flex justify-between items-center mb-4 border-b border-slate-800 pb-2">
                <span className="font-mono text-lg text-blue-400 font-bold">Topic: {TOPIC_NAME}</span>
                <span className="text-xs bg-slate-800 px-2 py-1 rounded-full text-slate-300">3 Partitions</span>
              </div>

              <div className="flex flex-col gap-4">
                {PARTITIONS.map(partition => {
                  const partitionMessages = messages.filter(m => m.partition === partition);
                  return (
                    <div key={partition} className="relative">
                      <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                        <span>Partition {partition}</span>
                        <span>Next Offset: {nextOffsets[partition]}</span>
                      </div>
                      <div className="h-16 bg-slate-900 border border-slate-700 rounded flex items-center p-2 overflow-x-auto gap-2 no-scrollbar">
                        {partitionMessages.length === 0 && (
                          <span className="text-slate-600 text-sm italic mx-auto">Empty</span>
                        )}
                        {partitionMessages.map(msg => (
                          <div 
                            key={msg.id} 
                            className="bg-orange-500/20 border border-orange-500/50 text-orange-200 px-3 py-1 rounded text-xs whitespace-nowrap animate-in fade-in zoom-in duration-300 flex flex-col items-center min-w-[80px]"
                          >
                            <span className="font-bold text-[10px] text-orange-400">offset: {msg.offset}</span>
                            <span className="font-mono truncate w-full text-center">{msg.text}</span>
                          </div>
                        ))}
                        {/* Dummy invisible div to keep scroll at right */}
                        <div ref={messagesEndRef} />
                      </div>

                      {/* Visual indicator for Consumer Group Offsets */}
                      <div className="mt-1 flex justify-between px-1">
                         <div className="text-[10px] flex gap-3">
                           <span className="text-green-400 flex items-center gap-1">
                             <div className="w-2 h-2 rounded-full bg-green-500"></div>
                             Analytics Read: {groupOffsets['Analytics'][partition]}
                           </span>
                           <span className="text-purple-400 flex items-center gap-1">
                             <div className="w-2 h-2 rounded-full bg-purple-500"></div>
                             Notifs Read: {groupOffsets['Notifications'][partition]}
                           </span>
                         </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {/* RIGHT COLUMN: CONSUMERS */}
        <div className="lg:col-span-4 flex flex-col gap-6">
          <div className="bg-slate-900 rounded-xl border border-slate-800 p-5 shadow-lg min-h-full">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-semibold flex items-center gap-2 text-white">
                <Users className="w-5 h-5 text-green-400" />
                Consumers
              </h2>
              {!autoPoll && (
                <button onClick={pollMessages} className="bg-green-600 hover:bg-green-500 text-white text-xs px-3 py-1.5 rounded transition-colors flex items-center gap-1">
                  Manual Poll
                </button>
              )}
            </div>

            {renderEducationalNote(
              "Consumer Groups", 
              "Consumers in the SAME group share partitions (Load Balancing). Multiple groups reading the same topic act as Pub-Sub (Broadcast)."
            )}

            {/* CONSUMER GROUPS */}
            {['Analytics', 'Notifications'].map(groupId => {
              const groupConsumers = consumers.filter(c => c.groupId === groupId);
              const isGreen = groupId === 'Analytics';

              return (
                <div key={groupId} className={`mt-4 border ${isGreen ? 'border-green-500/30 bg-green-950/10' : 'border-purple-500/30 bg-purple-950/10'} rounded-lg p-3`}>
                  <div className="flex justify-between items-center mb-3">
                    <h3 className={`font-bold ${isGreen ? 'text-green-400' : 'text-purple-400'} flex items-center gap-2`}>
                      Group: {groupId}
                    </h3>
                    <button 
                      onClick={() => addConsumer(groupId)}
                      className="p-1 hover:bg-slate-800 rounded text-slate-400 hover:text-white transition-colors"
                      title="Add Consumer to Group"
                    >
                      <Plus className="w-4 h-4" />
                    </button>
                  </div>

                  {groupConsumers.length === 0 ? (
                    <div className="text-center text-sm text-slate-500 py-4 italic">No consumers in this group.</div>
                  ) : (
                    <div className="space-y-3">
                      {groupConsumers.map(consumer => (
                        <div key={consumer.id} className="bg-slate-950 border border-slate-800 rounded p-3 relative overflow-hidden">
                          
                          <div className="flex justify-between items-start mb-2">
                            <div className="flex items-center gap-2">
                              <span className="font-mono font-bold text-slate-200">{consumer.id}</span>
                              <div className="flex gap-1">
                                {consumer.assignedPartitions.length > 0 ? (
                                  consumer.assignedPartitions.map(p => (
                                    <span key={p} className="text-[10px] bg-slate-800 text-slate-300 px-1.5 py-0.5 rounded">
                                      Part: {p}
                                    </span>
                                  ))
                                ) : (
                                  <span className="text-[10px] bg-red-900/50 text-red-300 px-1.5 py-0.5 rounded">
                                    Idle (No Partition)
                                  </span>
                                )}
                              </div>
                            </div>
                            <button onClick={() => removeConsumer(consumer.id)} className="text-slate-500 hover:text-red-400">
                              <Trash2 className="w-3.5 h-3.5" />
                            </button>
                          </div>

                          {/* Consumer Log */}
                          <div className="h-20 bg-slate-900 rounded p-2 overflow-y-auto text-xs font-mono">
                            {(!consumerLogs[consumer.id] || consumerLogs[consumer.id].length === 0) && (
                              <span className="text-slate-600">Waiting for messages...</span>
                            )}
                            {consumerLogs[consumer.id]?.map((log, i) => (
                              <div key={i} className="text-slate-300 border-b border-slate-800/50 last:border-0 py-0.5 flex justify-between">
                                <span className="truncate mr-2">"{log.text}"</span>
                                <span className="text-slate-500 shrink-0">p{log.partition}:o{log.offset}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                  
                  {/* Rebalancing Note */}
                  {groupConsumers.length > PARTITIONS.length && (
                    <div className="mt-2 text-[10px] text-red-400 flex items-center gap-1 bg-red-950/30 p-1.5 rounded">
                      <Info className="w-3 h-3" />
                      More consumers than partitions! Extra consumers sit idle.
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* DEADPOOL SIDEBAR OVERLAY */}
      {isSidebarOpen && (
        <div 
          className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40 transition-opacity" 
          onClick={() => setIsSidebarOpen(false)} 
        />
      )}

      {/* DEADPOOL SIDEBAR */}
      <div 
        className={`fixed top-0 left-0 h-full w-full sm:w-[450px] bg-slate-900 border-r border-red-900 shadow-2xl z-50 transform transition-transform duration-300 ease-in-out ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full'} flex flex-col`}
      >
         <div className="flex justify-between items-center p-4 border-b border-slate-800 bg-slate-950">
            <h2 className="text-xl font-bold text-red-500 flex items-center gap-2">
              🌮 Maximum Effort Guide
            </h2>
            <button 
              onClick={() => setIsSidebarOpen(false)} 
              className="p-2 text-slate-400 hover:text-white hover:bg-red-600 rounded transition-colors"
              title="Close Guide"
            >
              <X className="w-5 h-5" />
            </button>
         </div>
         
         <div className="p-6 overflow-y-auto flex-1 space-y-6 text-slate-300 text-sm leading-relaxed">
            <p className="text-lg text-slate-200">
              Look, I get it. You're staring at those little boxes moving across the screen and thinking, <span className="italic text-red-400">"Wade, what the [CENSORED] am I looking at?"</span>
            </p>
            <p>
              Forget servers and web clicks. Let's talk about my two favorite things: <strong>creating absolute chaos</strong>, and <strong>chimichangas</strong>.
            </p>
            <p>
              Imagine I just raided a warehouse full of heavily armed goons who work for a guy named Francis. I am creating a massive mess. Bodies everywhere, weapons dropped, pure cinematic beauty. Now, I need a system to clean this up, because I'm a hero, damn it.
            </p>
            <p className="font-bold text-red-400 text-lg border-b border-red-900/30 pb-2">Welcome to Apache Kafka.</p>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">1. The Producer (That's Me!)</h3>
              <p>In the simulator on your left, you see the "Producer." In our world, the Producer is <strong>ME</strong>. I am producing events. Every time I slice off a bad guy's arm, that's an "event." Every time I order a chimichanga on my phone mid-battle, that's an "event." The Producer's only job is to aggressively shove data into the system and walk away in slow motion.</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">2. The Topic ("The Warehouse")</h3>
              <p>I can't just throw chimichanga orders and severed arms into the same pile. That's unsanitary. So, Kafka uses <strong>Topics</strong>. A Topic is just a category.</p>
              <ul className="list-disc list-inside mt-2 text-slate-400 ml-2 space-y-1">
                <li>Topic A: <code className="bg-slate-800 text-red-300 px-1.5 py-0.5 rounded font-mono text-xs">dead-goons</code></li>
                <li>Topic B: <code className="bg-slate-800 text-red-300 px-1.5 py-0.5 rounded font-mono text-xs">chimichanga-orders</code></li>
              </ul>
              <p className="mt-2">In your UI, the Topic is called <code className="bg-slate-800 text-blue-300 px-1.5 py-0.5 rounded font-mono text-xs">web-clicks</code>. Boring, but same idea.</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">3. Partitions (The Dumpsters)</h3>
              <p>Here's the problem: If I drop 10,000 goons in one night, I can't stuff them all into one tiny dumpster. The dumpster will explode.</p>
              <p className="mt-2">So, Kafka splits the Topic into <strong>Partitions</strong>. Think of Partitions as multiple dumpsters lined up in the alley (Dumpster 0, Dumpster 1, Dumpster 2). When I produce a dead goon, Kafka randomly tosses him into one of the available dumpsters so the load is spread out. Look at the middle of your screen—see how the messages are split into 3 rows? Those are the dumpsters.</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">4. Offsets (The Toe Tags)</h3>
              <p>Once a goon goes into a dumpster, he gets a toe tag with a number. Goon #0, Goon #1, Goon #2. This is the <strong>Offset</strong>. It's a permanent ID. Once you are Goon #4 in Dumpster 2, you are <em>always</em> Goon #4 in Dumpster 2. You cannot cut in line. You cannot trade places. It is an immutable, glorious timeline of my handiwork.</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">5. Consumers (The Clean-up Crew)</h3>
              <p>Now, who is emptying these dumpsters? That's the <strong>Consumer</strong> (on the right side of your screen). Let's say I hire Dopinder (my cab driver) to process the <code className="bg-slate-800 text-red-300 px-1.5 py-0.5 rounded font-mono text-xs">dead-goons</code> topic. He pulls up to Dumpster 0, reads Goon #0, disposes of him, and moves to Goon #1. He keeps track of his progress so if he takes a bathroom break, he knows exactly where he left off.</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">6. Consumer Groups (The Team)</h3>
              <p>Poor Dopinder is getting overwhelmed. Three overflowing dumpsters, and he's just one guy. This is where <strong>Consumer Groups</strong> come in to save the day!</p>
              <p className="mt-2">A Consumer Group is a team. I hire Weasel and Blind Al to help Dopinder. Now we have a group of 3 people, and guess what?</p>
              <ul className="list-disc list-inside mt-2 text-slate-400 ml-2 space-y-1">
                <li>Dopinder takes Dumpster 0.</li>
                <li>Weasel takes Dumpster 1.</li>
                <li>Blind Al takes Dumpster 2.</li>
              </ul>
              <p className="mt-3 text-green-400 font-bold">This is the magic of Kafka scaling!</p>
              <p>They are splitting the work perfectly. (Try this in your UI: add 3 consumers to the Analytics group and watch them split the partitions!).</p>
            </div>

            <div className="bg-red-950/40 border border-red-900 rounded-lg p-4 shadow-inner">
              <h3 className="text-red-500 font-bold flex items-center gap-2 uppercase tracking-wider mb-2">
                🚨 The Golden Rule 🚨
              </h3>
              <p>What happens if I get cocky and hire a 4th person (let's say, Wolverine) for the clean-up crew? We only have <strong>3 Dumpsters</strong> (Partitions).</p>
              <p className="mt-2">If Logan joins the group, he gets <strong>nothing</strong>. He just stands there, flexing his claws, looking grumpy, totally <em>idle</em>.</p>
              <p className="mt-3 font-bold text-red-200">Rule: You can NEVER have more active consumers in a single group than you have partitions. (Go ahead, add a 4th consumer to the UI and watch it say "Idle").</p>
            </div>

            <div>
              <h3 className="text-red-400 font-bold text-base mb-1">7. Multiple Groups (Pub/Sub)</h3>
              <p>Wait, what if someone else wants this data? Let's say the NYPD <em>also</em> wants to look at the topic to write police reports.</p>
              <p className="mt-2">They don't join Dopinder's clean-up crew. They form their <em>own</em> Consumer Group (like "Notifications" in your UI). Now, Dopinder's team reads the dumpsters to burn the bodies, and the NYPD team reads the <em>exact same dumpsters</em> to take photos. They don't interfere with each other. They each keep their own separate bookmarks of where they left off.</p>
            </div>
            
            <p className="italic text-center text-slate-500 pt-6 pb-4 border-t border-slate-800/50 mt-8">
              Now hit that "Auto Produce" button, watch the little boxes fly, and pretend they are chimichangas. You've earned it!
            </p>
         </div>
      </div>
    </div>
  );
}