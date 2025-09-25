import requests
import json
import time
import threading
from typing import Dict, List, Optional

# Configuración
BASE_URL = "http://localhost:8000"
TEST_CONFIG = {
    "timeout": 30,
    "poll_interval": 2
}

class APITester:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.consulta_ids = []  # Para guardar IDs de consultas de prueba
    
    def print_separator(self, title: str):
        """Imprime un separador con título"""
        print(f"\n{'='*60}")
        print(f"🧪 {title}")
        print(f"{'='*60}")
    
    def test_health(self) -> bool:
        """Prueba que la API esté funcionando"""
        try:
            response = requests.get(f"{self.base_url}/")
            if response.status_code == 200:
                print("✅ API está funcionando")
                return True
            else:
                print("❌ API no responde correctamente")
                return False
        except requests.exceptions.ConnectionError:
            print("❌ No se puede conectar a la API. ¿Está ejecutándose?")
            print(f"   Ejecuta: uvicorn main:app --reload --port 8000")
            return False
    
    def test_config_endpoint(self) -> bool:
        """Prueba el endpoint de configuración"""
        try:
            response = requests.get(f"{self.base_url}/api/config")
            if response.status_code == 200:
                config = response.json()
                print("✅ Config endpoint funciona")
                print(f"   Satélites: {len(config['satellites']['validos'])}")
                print(f"   Bandas: {len(config['bandas']['validas'])}")
                print(f"   Productos: {len(config['products']['validos'])}")
                return True
            return False
        except Exception as e:
            print(f"❌ Error en config endpoint: {e}")
            return False
    
    def crear_consulta_simple(self) -> Optional[str]:
        """Crea una consulta simple y devuelve el ID"""
        query_data = {
            "nivel": "L1b",
            "bandas": ["ALL"],
            "fechas": {
                "20240101": ["09:00-12:00"]
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/query",
                json=query_data,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                consulta_id = data["consulta_id"]
                self.consulta_ids.append(consulta_id)
                print(f"✅ Consulta creada: {consulta_id}")
                print(f"   Resumen: {data['resumen']}")
                return consulta_id
            else:
                print(f"❌ Error creando consulta: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def crear_consulta_compleja(self) -> Optional[str]:
        """Crea una consulta más compleja"""
        query_data = {
            "sat": "GOES-EAST",
            "nivel": "L2",
            "dominio": "conus",
            "productos": ["CMIP", "ACTP", "Rainfall"],
            "bandas": ["13", "08", "11"],
            "fechas": {
                "20240101-20240103": ["09:00-12:00", "14:00-16:00"],
                "20240105": ["10:00-11:00"]
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/query",
                json=query_data,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                consulta_id = data["consulta_id"]
                self.consulta_ids.append(consulta_id)
                print(f"✅ Consulta compleja creada: {consulta_id}")
                print(f"   Total fechas: {data['resumen']['total_fechas']}")
                print(f"   Total horas: {data['resumen']['total_horas']}")
                return consulta_id
            else:
                print(f"❌ Error creando consulta compleja: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def crear_consulta_con_error(self):
        """Crea una consulta con error para probar manejo de errores"""
        query_data = {
            "nivel": "L1b",
            "bandas": ["99"],  # Banda inválida
            "fechas": {
                "20240101": ["09:00-12:00"]
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/query",
                json=query_data,
                timeout=10
            )
            
            if response.status_code == 400:
                print("✅ Manejo de errores funciona correctamente")
                print(f"   Error esperado: {response.json()['detail']}")
                return True
            else:
                print("❌ Se esperaba un error pero la consulta fue exitosa")
                return False
                
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def monitorear_consulta(self, consulta_id: str, timeout: int = 30) -> bool:
        """Monitorea el progreso de una consulta"""
        print(f"🔍 Monitoreando consulta {consulta_id}...")
        
        start_time = time.time()
        estados_observados = []
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.base_url}/api/query/{consulta_id}")
                
                if response.status_code == 200:
                    estado = response.json()
                    estado_actual = estado.get("estado", "desconocido")
                    progreso = estado.get("progreso", 0)
                    mensaje = estado.get("mensaje", "")
                    
                    if estado_actual not in estados_observados:
                        estados_observados.append(estado_actual)
                        print(f"   📊 Estado: {estado_actual}, Progreso: {progreso}%")
                        print(f"   💬 Mensaje: {mensaje}")
                    
                    if estado_actual == "completado":
                        print("✅ Consulta completada exitosamente!")
                        return True
                    elif estado_actual == "error":
                        print("❌ Consulta falló")
                        print(f"   Error: {mensaje}")
                        return False
                
                time.sleep(TEST_CONFIG["poll_interval"])
                
            except Exception as e:
                print(f"❌ Error monitoreando consulta: {e}")
                return False
        
        print("⏰ Timeout esperando por la consulta")
        return False
    
    def obtener_resultados(self, consulta_id: str) -> bool:
        """Obtiene los resultados de una consulta completada"""
        try:
            response = requests.get(f"{self.base_url}/api/query/{consulta_id}/resultados")
            
            if response.status_code == 200:
                resultados = response.json()
                print("✅ Resultados obtenidos:")
                print(f"   Archivos generados: {len(resultados['resultados']['archivos_generados'])}")
                print(f"   Tamaño total: {resultados['resultados']['tamaño_total_mb']} MB")
                print(f"   URL descarga: {resultados['resultados']['url_descarga']}")
                return True
            else:
                print(f"❌ Error obteniendo resultados: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def listar_consultas(self):
        """Lista todas las consultas"""
        try:
            response = requests.get(f"{self.base_url}/api/queries")
            
            if response.status_code == 200:
                data = response.json()
                print(f"📋 Total consultas en sistema: {data['total_consultas']}")
                
                for consulta in data["consultas"][:5]:  # Mostrar primeras 5
                    print(f"   - {consulta['id']}: {consulta['estado']} ({consulta['satelite']})")
                return True
            return False
        except Exception as e:
            print(f"❌ Error listando consultas: {e}")
            return False
    
    def test_concurrente(self, num_consultas: int = 3):
        """Prueba crear múltiples consultas concurrentes"""
        self.print_separator(f"TEST CONCURRENTE ({num_consultas} consultas)")
        
        consulta_ids = []
        threads = []
        
        def crear_consulta_thread(index: int):
            query_data = {
                "nivel": "L1b",
                "bandas": ["13", "08"],
                "fechas": {
                    f"2024010{index+1}": [f"0{index+9}:00-1{index+1}:00"]
                }
            }
            
            try:
                response = requests.post(f"{self.base_url}/api/query", json=query_data)
                if response.status_code == 200:
                    consulta_id = response.json()["consulta_id"]
                    consulta_ids.append(consulta_id)
                    print(f"   ✅ Consulta {index+1} creada: {consulta_id}")
            except Exception as e:
                print(f"   ❌ Error en consulta {index+1}: {e}")
        
        # Crear consultas en threads separados
        for i in range(num_consultas):
            thread = threading.Thread(target=crear_consulta_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Esperar que todos los threads terminen
        for thread in threads:
            thread.join()
        
        print(f"\n📊 Consultas creadas: {len(consulta_ids)}/{num_consultas}")
        return consulta_ids
    
    def ejecutar_todas_las_pruebas(self):
        """Ejecuta todas las pruebas en secuencia"""
        print("🚀 INICIANDO PRUEBAS DEL API /api/query")
        print(f"📡 URL base: {self.base_url}")
        
        # 1. Prueba básica de conectividad
        self.print_separator("PRUEBA DE CONECTIVIDAD")
        if not self.test_health():
            return False
        
        # 2. Prueba configuración
        self.print_separator("PRUEBA DE CONFIGURACIÓN")
        if not self.test_config_endpoint():
            return False
        
        # 3. Prueba manejo de errores
        self.print_separator("PRUEBA DE MANEJO DE ERRORES")
        self.crear_consulta_con_error()
        
        # 4. Prueba consulta simple
        self.print_separator("PRUEBA CONSULTA SIMPLE")
        consulta_id_simple = self.crear_consulta_simple()
        if consulta_id_simple:
            self.monitorear_consulta(consulta_id_simple)
            self.obtener_resultados(consulta_id_simple)
        
        # 5. Prueba consulta compleja
        self.print_separator("PRUEBA CONSULTA COMPLEJA")
        consulta_id_compleja = self.crear_consulta_compleja()
        if consulta_id_compleja:
            self.monitorear_consulta(consulta_id_compleja)
            self.obtener_resultados(consulta_id_compleja)
        
        # 6. Prueba concurrente
        self.print_separator("PRUEBA CONCURRENTE")
        consultas_concurrentes = self.test_concurrente(3)
        
        # 7. Listar todas las consultas
        self.print_separator("LISTADO DE CONSULTAS")
        self.listar_consultas()
        
        # 8. Prueba de estadísticas
        self.print_separator("ESTADÍSTICAS DEL SISTEMA")
        try:
            response = requests.get(f"{self.base_url}/api/procesador/estadisticas")
            if response.status_code == 200:
                stats = response.json()
                print(f"📊 Estadísticas del procesador:")
                for key, value in stats.items():
                    print(f"   {key}: {value}")
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
        
        self.print_separator("PRUEBAS COMPLETADAS")
        print("🎉 Todas las pruebas ejecutadas. Revisa los resultados arriba.")
        
        return True

# Ejecución rápida de prueba individual
def prueba_rapida():
    """Prueba rápida para desarrollo"""
    tester = APITester()
    
    if tester.test_health():
        # Crear una consulta simple y monitorearla
        consulta_id = tester.crear_consulta_simple()
        if consulta_id:
            tester.monitorear_consulta(consulta_id, timeout=20)

if __name__ == "__main__":
    import sys
    
    tester = APITester()
    
    if len(sys.argv) > 1 and sys.argv[1] == "rapida":
        prueba_rapida()
    else:
        tester.ejecutar_todas_las_pruebas()