import { useEffect, useState } from 'react'
import { supabase } from './supabaseClient'

function App() {
  const [message, setMessage] = useState("Connecting to Supabase...")

  useEffect(() => {
    async function testConnection() {
      // Try to read 1 row from your table
      const { data, error } = await supabase
        .from('columbia_data')   // <-- YOUR TABLE NAME
        .select('*')
        .limit(1)

      if (error) {
        console.error("Supabase error:", error)
        setMessage(`❌ Supabase connection FAILED: ${error.message}`)
      } else {
        console.log("Supabase data:", data)
        setMessage("✅ Supabase connection SUCCESSFUL")
      }
    }

    testConnection()
  }, [])

  return (
    <div style={{ 
      fontFamily: "Arial", 
      padding: "40px", 
      fontSize: "24px" 
    }}>
      {message}
    </div>
  )
}

export default App