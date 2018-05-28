package neutron

import (
	uuid "github.com/satori/go.uuid"
)

type FixedIP struct {
	IP       string    `json:"ip_address"`
	SubnetID uuid.UUID `json:"subnet_id"`
}

type AAP struct {
	IP  string `json:"ip_address"`
	MAC string `json:"mac_address"`
}

type Port struct {
	ID             uuid.UUID   `json:"id"`
	TenantID       string      `json:"tenant_id"`
	NetworkID      uuid.UUID   `json:"network_id"`
	Name           string      `json:"name"`
	Description    string      `json:"description"`
	SecurityGroups []uuid.UUID `json:"security_groups"`
	FixedIPs       []FixedIP   `json:"fixed_ips"`
	MAC            string      `json:"mac_address"`
	AAPs           []AAP       `json:"allowed_address_pairs"`
	DeviceID       string      `json:"device_id"`
	DeviceOwner    string      `json:"device_owner"`
	Status         string      `json:"status"`
	AdminStateUp   bool        `json:"admin_state_up"`
	CreatedAt      string      `json:"created_at"`
	UpdatedAt      string      `json:"updated_at"`
}

type Network struct {
	ID                  uuid.UUID   `json:"id"`
	TenantID            string      `json:"tenant_id"`
	Name                string      `json:"name"`
	Description         string      `json:"description"`
	RouterExternal      bool        `json:"router:external"`
	Shared              bool        `json:"shared"`
	Subnets             []uuid.UUID `json:"subnets"`
	PortSecurityEnabled bool        `json:"port_security_enabled"`
	Status              string      `json:"status"`
	AdminStateUp        bool        `json:"admin_state_up"`
	CreatedAt           string      `json:"created_at"`
	UpdatedAt           string      `json:"updated_at"`
}
